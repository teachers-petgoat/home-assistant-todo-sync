"""Bidirectional snapshot-based synchronization engine."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from homeassistant.components.todo import (
    DATA_COMPONENT,
    TodoItem,
    TodoItemStatus,
    TodoListEntity,
)
from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_track_state_change_event

from .const import CONF_ENTITY_A_REGISTRY_ID, CONF_ENTITY_B_REGISTRY_ID
from .models import ItemPair, Side, Snapshot, SyncItem, snapshot
from .storage import MappingStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.helpers.entity_component import EntityComponent

_LOGGER = logging.getLogger(__name__)

# The first confirmation plus four out-of-order callbacks are protected. Most
# providers settle sooner through consecutive confirmation; this is a strict
# memory/suppression bound rather than a time-based delay.
SETTLING_MAX_GENERATIONS = 5
SETTLING_CONSECUTIVE_OBSERVATIONS = 2

# Cloud providers can briefly omit an item from otherwise complete snapshots.
# Destructive propagation requires a later observation outside this interval.
REMOVAL_STABILIZATION_SECONDS = 5.0
RECENT_ITEM_STABILIZATION_SECONDS = 5.0


class SyncSetupError(RuntimeError):
    """Raised when configured entity-registry references cannot be resolved."""


class ExpectedOperationType(StrEnum):
    """A mutation initiated by Todo Sync that may produce an echo callback."""

    CREATE = "create"
    UPDATE_STATUS = "update_status"
    RENAME = "rename"
    DELETE = "delete"


class OperationSnapshotState(StrEnum):
    """How a provider snapshot relates to an internal mutation."""

    CONFIRMED = "confirmed"
    STALE = "stale"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class ExpectedOperation:
    """The exact before/after mutation expected from one provider."""

    operation: ExpectedOperationType
    before: SyncItem | None
    after: SyncItem | None
    before_uids: frozenset[str] = frozenset()
    source_side: Side | None = None
    source_uid: str | None = None
    pair_id: str | None = None

    @property
    def normalized_summary(self) -> str:
        """Return the logical item key involved in this operation."""
        item = self.after or self.before
        return item.normalized_summary if item else ""


@dataclass(slots=True)
class SettlingOperation:
    """A confirmed operation awaiting evidence that callbacks have settled."""

    expected: ExpectedOperation
    confirmed_generation: int
    confirmed_observations: int = 1
    inverse_observations: int = 0


@dataclass(slots=True)
class RemovalCandidate:
    """A mapped disappearance awaiting independent destructive confirmation."""

    side: Side
    uid: str
    pair_id: str
    previous: SyncItem
    first_missing_generation: int
    first_missing_monotonic: float
    missing_observations: int = 1


@dataclass(slots=True)
class RenameCandidate:
    """A possible provider UID-replacement rename awaiting stabilization."""

    side: Side
    pair_id: str
    old: SyncItem
    replacement: SyncItem
    first_missing_generation: int
    first_missing_monotonic: float
    first_replacement_generation: int


@dataclass(slots=True)
class DeferredChange:
    """The first baseline and latest desired state for a missing target."""

    source: Side
    baseline: SyncItem
    desired: SyncItem


class TodoSyncEngine:
    """Synchronize two TodoListEntity instances using complete snapshots."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the engine."""
        self.hass = hass
        self.entry = entry
        self.store = MappingStore(hass, entry.entry_id)
        self.entities: dict[Side, TodoListEntity] = {}
        self.entity_ids: dict[Side, str] = {}
        self.snapshots: dict[Side, Snapshot] = {Side.A: {}, Side.B: {}}
        self._unsub_updates: list[Callable[[], None]] = []
        self._unsub_state: Callable[[], None] | None = None
        self._unsub_registry: Callable[[], None] | None = None
        self._lock = asyncio.Lock()
        self._stopped = False
        self._available: dict[Side, bool] = {Side.A: False, Side.B: False}
        self._expected: dict[Side, list[ExpectedOperation]] = {
            Side.A: [],
            Side.B: [],
        }
        # Confirmed operations remain as non-pending stale guards. Cloud providers
        # can deliver an older snapshot even after delivering the confirmation.
        self._confirmed: dict[Side, list[SettlingOperation]] = {
            Side.A: [],
            Side.B: [],
        }
        self._generation: dict[Side, int] = {Side.A: 0, Side.B: 0}
        self._removal_candidates: dict[tuple[Side, str], RemovalCandidate] = {}
        self._recent_items: dict[tuple[Side, str], float] = {}
        self._rename_candidates: dict[tuple[Side, str], RenameCandidate] = {}
        self._deferred_changes: dict[str, DeferredChange] = {}
        self._unresolved_deletes: dict[str, Side] = {}

    async def async_start(self) -> None:
        """Resolve entities, load state, subscribe, and establish a baseline."""
        await self.store.async_load()
        self._resolve_entities()
        self._subscribe()
        if self._both_available():
            await self.async_reconcile()

    async def async_stop(self) -> None:
        """Remove every listener and let any active operation finish."""
        self._stopped = True
        for unsubscribe in self._unsub_updates:
            unsubscribe()
        self._unsub_updates.clear()
        if self._unsub_state:
            self._unsub_state()
            self._unsub_state = None
        if self._unsub_registry:
            self._unsub_registry()
            self._unsub_registry = None
        async with self._lock:
            await self.store.async_save()

    def _resolve_entities(self) -> None:
        """Resolve current entity IDs from durable registry entry IDs."""
        registry = er.async_get(self.hass)
        component = cast(
            "EntityComponent[TodoListEntity] | None", self.hass.data.get(DATA_COMPONENT)
        )
        if component is None:
            msg = "The Home Assistant todo component is not loaded"
            raise SyncSetupError(msg)
        resolved_entities: dict[Side, TodoListEntity] = {}
        resolved_ids: dict[Side, str] = {}
        for side, key in (
            (Side.A, CONF_ENTITY_A_REGISTRY_ID),
            (Side.B, CONF_ENTITY_B_REGISTRY_ID),
        ):
            registry_id = self.entry.data.get(key)
            registry_entry = (
                registry.async_get(registry_id)
                if isinstance(registry_id, str)
                else None
            )
            if registry_entry is None:
                msg = f"Todo Sync {side} registry entry no longer exists"
                raise SyncSetupError(msg)
            entity_id = registry_entry.entity_id
            entity = component.get_entity(entity_id)
            if not isinstance(entity, TodoListEntity):
                msg = f"{entity_id} is not a loaded TodoListEntity"
                raise SyncSetupError(msg)
            resolved_ids[side] = entity_id
            resolved_entities[side] = entity
        self.entity_ids = resolved_ids
        self.entities = resolved_entities
        self._update_availability()

    def _subscribe(self) -> None:
        """Subscribe directly to full TodoListEntity updates and availability."""
        for unsubscribe in self._unsub_updates:
            unsubscribe()
        self._unsub_updates = []
        for side, entity in self.entities.items():

            @callback
            def receive(
                items: list[TodoItem] | None, watched_side: Side = side
            ) -> None:
                if self._stopped or items is None:
                    return
                self.hass.async_create_task(
                    self._async_receive(watched_side, snapshot(items))
                )

            self._unsub_updates.append(entity.async_subscribe_updates(receive))
        if self._unsub_state:
            self._unsub_state()
        self._unsub_state = async_track_state_change_event(
            self.hass, list(self.entity_ids.values()), self._state_changed
        )
        if self._unsub_registry is None:
            self._unsub_registry = self.hass.bus.async_listen(
                er.EVENT_ENTITY_REGISTRY_UPDATED, self._registry_changed
            )

    @callback
    def _state_changed(self, _event: Event[Any]) -> None:
        """Reconcile after an entity recovers; never equate unavailable with empty."""
        old = dict(self._available)
        self._update_availability()
        if self._both_available() and not all(old.values()):
            self.hass.async_create_task(self.async_reconcile())

    @callback
    def _registry_changed(self, event: Event[Any]) -> None:
        """Rebind subscriptions when either registry entry changes entity ID."""
        if event.data.get("action") != "update":
            return
        if not {
            event.data.get("entity_id"),
            event.data.get("old_entity_id"),
        }.intersection(self.entity_ids.values()):
            return
        self.hass.async_create_task(self._async_rebind())

    async def _async_rebind(self) -> None:
        async with self._lock:
            try:
                self._resolve_entities()
            except SyncSetupError:
                _LOGGER.warning("Unable to re-resolve Todo Sync entities")
                return
            self._subscribe()
        if self._both_available():
            await self.async_reconcile()

    def _update_availability(self) -> None:
        for side, entity_id in self.entity_ids.items():
            state = self.hass.states.get(entity_id)
            self._available[side] = bool(
                state and state.state not in (STATE_UNAVAILABLE, STATE_UNKNOWN)
            )

    def _both_available(self) -> bool:
        return all(self._available.values())

    def _current(self, side: Side) -> Snapshot:
        return snapshot(self.entities[side].todo_items or ())

    async def _async_receive(self, side: Side, current: Snapshot) -> None:
        async with self._lock:
            if self._stopped:
                return
            self._update_availability()
            if not self._both_available():
                _LOGGER.debug("Deferring Todo Sync update while a list is unavailable")
                return
            self._generation[side] += 1
            self._cleanup_recent_items()
            _LOGGER.debug(
                "Received Todo Sync snapshot generation %d for side %s (%d items)",
                self._generation[side],
                side,
                len(current),
            )
            previous = self.snapshots[side]
            pending_count = len(self._expected[side])
            effective, authoritative = self._classify_and_mask_expected(
                side, previous, current
            )
            if len(self._expected[side]) != pending_count:
                await self.store.async_save()
            if effective == previous:
                if any(
                    candidate_side is side
                    for candidate_side, _uid in self._removal_candidates
                ):
                    await self._apply_delta(side, previous, effective)
                self.snapshots[side] = authoritative
                return
            await self._apply_delta(side, previous, effective)
            self.snapshots[side] = authoritative

    def _expect(self, side: Side, operation: ExpectedOperation) -> None:
        """Record an internal operation without advancing provider authority."""
        self._expected[side].append(operation)
        _LOGGER.debug(
            "Pending expected %s created for side %s summary %r",
            operation.operation,
            side,
            operation.normalized_summary,
        )

    def _classify_operation(  # noqa: C901, PLR0911
        self, operation: ExpectedOperation, current: Snapshot
    ) -> tuple[OperationSnapshotState, set[str]]:
        """Classify one snapshot and return provider UIDs affected by it."""
        before, after = operation.before, operation.after
        if operation.operation is ExpectedOperationType.CREATE:
            candidates = {
                item.uid
                for item in current.values()
                if item.uid not in operation.before_uids
                and after is not None
                and item.normalized_summary == after.normalized_summary
                and item.status is after.status
            }
            if len(candidates) == 1:
                return OperationSnapshotState.CONFIRMED, candidates
            if not candidates:
                return OperationSnapshotState.STALE, set()
            return OperationSnapshotState.CONFLICT, candidates
        if before is None:
            return OperationSnapshotState.CONFLICT, set()
        if operation.operation is ExpectedOperationType.DELETE:
            value = current.get(before.uid)
            if value == before:
                return OperationSnapshotState.STALE, {before.uid}
            if value is None:
                return OperationSnapshotState.CONFIRMED, {before.uid}
            return OperationSnapshotState.CONFLICT, {before.uid}
        if current.get(before.uid) == before:
            return OperationSnapshotState.STALE, {before.uid}
        if after is None:
            return OperationSnapshotState.CONFLICT, {before.uid}
        if current.get(after.uid) == after:
            return OperationSnapshotState.CONFIRMED, {before.uid, after.uid}
        candidates = {
            item.uid
            for item in current.values()
            if item.normalized_summary == after.normalized_summary
            and item.status is after.status
            and item.uid != before.uid
        }
        if operation.operation is ExpectedOperationType.RENAME and len(candidates) == 1:
            return OperationSnapshotState.CONFIRMED, {before.uid, *candidates}
        return OperationSnapshotState.CONFLICT, {before.uid, *candidates}

    def _mask_operation(
        self,
        operation: ExpectedOperation,
        affected: set[str],
        previous: Snapshot,
        effective: Snapshot,
    ) -> None:
        """Restore only an operation's affected region from the prior baseline."""
        affected.update(
            uid
            for uid, item in effective.items()
            if uid not in operation.before_uids
            and item.normalized_summary == operation.normalized_summary
        )
        if operation.after is not None and operation.after.uid:
            affected.add(operation.after.uid)
        for uid in affected:
            if uid in previous:
                effective[uid] = previous[uid]
            else:
                effective.pop(uid, None)
        if operation.before is not None and operation.before.uid in previous:
            effective[operation.before.uid] = previous[operation.before.uid]

    def _adopt_confirmed(
        self, side: Side, operation: ExpectedOperation, uid: str
    ) -> None:
        """Adopt a provider-assigned UID after create or replacement rename."""
        if uid:
            self._recent_items[(side, uid)] = time.monotonic()
        if operation.operation is ExpectedOperationType.CREATE:
            if operation.source_side is None or operation.source_uid is None:
                return
            pair = ItemPair(
                operation.pair_id or str(uuid4()),
                "",
                "",
                operation.normalized_summary,
            )
            pair.set_uid(operation.source_side, operation.source_uid)
            pair.set_uid(side, uid)
            self.store.pairs[pair.pair_id] = pair
            _LOGGER.debug("Unresolved create adopted as %s:%s", side, uid)
            return
        if operation.pair_id and operation.pair_id in self.store.pairs:
            self.store.pairs[operation.pair_id].set_uid(side, uid)

    def _classify_and_mask_expected(  # noqa: C901, PLR0912
        self, side: Side, previous: Snapshot, current: Snapshot
    ) -> tuple[Snapshot, Snapshot]:
        """Mask stale/internal differences while preserving unrelated changes."""
        effective = dict(current)
        authoritative = dict(current)

        # A provider is considered settled after two consecutive observations of
        # the confirmed state. Conversely, two consecutive observations of the
        # pre-operation state retire the guard and allow the second observation
        # through as a genuine inverse user action.
        settling: list[SettlingOperation] = []
        for guard in self._confirmed[side]:
            if (
                self._generation[side] - guard.confirmed_generation
                >= SETTLING_MAX_GENERATIONS
            ):
                _LOGGER.debug(
                    "Expected %s retired at stabilization generation limit on side %s",
                    guard.expected.operation,
                    side,
                )
                continue
            state, affected = self._classify_operation(guard.expected, current)
            if state is OperationSnapshotState.CONFIRMED:
                guard.confirmed_observations += 1
                guard.inverse_observations = 0
                self._mask_operation(guard.expected, affected, previous, effective)
                if guard.confirmed_observations < SETTLING_CONSECUTIVE_OBSERVATIONS:
                    settling.append(guard)
                else:
                    _LOGGER.debug(
                        "Expected %s retired after provider settled on side %s",
                        guard.expected.operation,
                        side,
                    )
                continue
            if state is OperationSnapshotState.STALE:
                guard.confirmed_observations = 0
                guard.inverse_observations += 1
                if guard.inverse_observations < SETTLING_CONSECUTIVE_OBSERVATIONS:
                    self._mask_operation(guard.expected, affected, previous, effective)
                    self._mask_operation(
                        guard.expected, affected, previous, authoritative
                    )
                    settling.append(guard)
                    _LOGGER.debug(
                        "Post-confirmation stale snapshot masked for %s on side %s",
                        guard.expected.operation,
                        side,
                    )
                else:
                    _LOGGER.debug(
                        "Expected %s retired after repeated inverse state on side %s",
                        guard.expected.operation,
                        side,
                    )
                continue
            # An unrelated third state is not the known stale callback. Stop
            # allowing historical operation state to influence future deltas.
            _LOGGER.debug(
                "Expected %s retired on new provider state for side %s",
                guard.expected.operation,
                side,
            )
        self._confirmed[side] = settling

        remaining: list[ExpectedOperation] = []
        for operation in self._expected[side]:
            state, affected = self._classify_operation(operation, current)
            if state is OperationSnapshotState.CONFIRMED:
                uid = next(
                    (value for value in affected if value in current),
                    operation.after.uid if operation.after else "",
                )
                self._adopt_confirmed(side, operation, uid)
                confirmed = operation
                if uid in current and operation.operation in (
                    ExpectedOperationType.CREATE,
                    ExpectedOperationType.RENAME,
                ):
                    confirmed = replace(operation, after=current[uid])
                self._confirmed[side].append(
                    SettlingOperation(confirmed, self._generation[side])
                )
                _LOGGER.debug(
                    "Expected %s confirmed on side %s", operation.operation, side
                )
            else:
                remaining.append(operation)
                if state is OperationSnapshotState.STALE:
                    _LOGGER.debug(
                        "Stale snapshot masked for expected %s on side %s",
                        operation.operation,
                        side,
                    )
                else:
                    _LOGGER.warning(
                        "Expected %s conflict on side %s requires reconciliation",
                        operation.operation,
                        side,
                    )
            self._mask_operation(operation, affected, previous, effective)
            if state is not OperationSnapshotState.CONFIRMED:
                self._mask_operation(operation, affected, previous, authoritative)
        self._expected[side] = remaining
        return effective, authoritative

    def _pair_for_uid(self, side: Side, uid: str) -> ItemPair | None:
        return next(
            (pair for pair in self.store.pairs.values() if pair.uid(side) == uid), None
        )

    def _cleanup_recent_items(self) -> None:
        """Discard expired timestamps not referenced by active safety state."""
        now = time.monotonic()
        protected = set(self._removal_candidates)
        for side in (Side.A, Side.B):
            for operation in (
                *self._expected[side],
                *(guard.expected for guard in self._confirmed[side]),
            ):
                for item in (operation.before, operation.after):
                    if item is not None and item.uid:
                        protected.add((side, item.uid))
                if operation.source_side is not None and operation.source_uid:
                    protected.add((operation.source_side, operation.source_uid))
        self._recent_items = {
            key: observed
            for key, observed in self._recent_items.items()
            if key in protected or now - observed <= RECENT_ITEM_STABILIZATION_SECONDS
        }

    def _operation_protects_removal(self, uid: str, pair_id: str) -> bool:
        """Return whether create/rename operation context makes absence unsafe."""
        protected = (ExpectedOperationType.CREATE, ExpectedOperationType.RENAME)
        operations = [
            *(operation for values in self._expected.values() for operation in values),
            *(
                guard.expected
                for values in self._confirmed.values()
                for guard in values
            ),
        ]
        return any(
            operation.operation in protected
            and (
                operation.pair_id == pair_id
                or operation.source_uid == uid
                or (operation.before is not None and operation.before.uid == uid)
                or (operation.after is not None and operation.after.uid == uid)
            )
            for operation in operations
        )

    def _observe_removal(self, side: Side, pair: ItemPair, previous: SyncItem) -> bool:
        """Quarantine an omission and return whether deletion is now confirmed."""
        uid = previous.uid
        if self._operation_protects_removal(uid, pair.pair_id):
            _LOGGER.debug(
                "Removal remains protected by pending/settling operation for %s:%s",
                side,
                uid,
            )
            return False
        now = time.monotonic()
        key = (side, uid)
        candidate = self._removal_candidates.get(key)
        if candidate is None:
            self._removal_candidates[key] = RemovalCandidate(
                side=side,
                uid=uid,
                pair_id=pair.pair_id,
                previous=previous,
                first_missing_generation=self._generation[side],
                first_missing_monotonic=now,
            )
            _LOGGER.debug(
                "Removal candidate created for %s:%s pair %s at generation %d",
                side,
                uid,
                pair.pair_id,
                self._generation[side],
            )
            return False
        candidate.missing_observations += 1
        elapsed = now - candidate.first_missing_monotonic
        recent_elapsed = now - self._recent_items.get(key, float("-inf"))
        _LOGGER.debug(
            "Removal candidate %s:%s missing observation count %d",
            side,
            uid,
            candidate.missing_observations,
        )
        if (
            elapsed < REMOVAL_STABILIZATION_SECONDS
            or recent_elapsed < RECENT_ITEM_STABILIZATION_SECONDS
        ):
            _LOGGER.debug(
                "Removal %s:%s still inside stabilization window (%.3fs)",
                side,
                uid,
                elapsed,
            )
            return False
        return True

    def _cancel_or_repair_removal_candidates(
        self, side: Side, current: Snapshot
    ) -> tuple[set[str], list[tuple[ItemPair, SyncItem, SyncItem]]]:
        """Cancel returned omissions or adopt an unambiguous replacement UID."""
        handled_additions: set[str] = set()
        returned_changes: list[tuple[ItemPair, SyncItem, SyncItem]] = []
        for key, candidate in list(self._removal_candidates.items()):
            if candidate.side is not side:
                continue
            if candidate.uid in current:
                self._removal_candidates.pop(key)
                self._rename_candidates.pop((side, candidate.pair_id), None)
                handled_additions.add(candidate.uid)
                _LOGGER.debug(
                    "Removal candidate cancelled because item returned for %s:%s",
                    side,
                    candidate.uid,
                )
                pair = self.store.pairs.get(candidate.pair_id)
                returned = current[candidate.uid]
                if pair is not None and returned != candidate.previous:
                    returned_changes.append((pair, candidate.previous, returned))
                continue
            replacements = [
                item
                for item in current.values()
                if item.normalized_summary == candidate.previous.normalized_summary
                and item.status is candidate.previous.status
                and self._pair_for_uid(side, item.uid) is None
            ]
            if len(replacements) != 1:
                continue
            pair = self.store.pairs.get(candidate.pair_id)
            if pair is None:
                continue
            replacement = replacements[0]
            pair.set_uid(side, replacement.uid)
            self._removal_candidates.pop(key)
            self._recent_items[(side, replacement.uid)] = time.monotonic()
            handled_additions.add(replacement.uid)
            _LOGGER.debug(
                "Mapping repaired after replacement UID %s:%s -> %s",
                side,
                candidate.uid,
                replacement.uid,
            )
        return handled_additions, returned_changes

    async def _apply_deferred_changes(self, recovered_side: Side) -> None:
        """Apply logical changes after a previously missing mapped target returns."""
        for pair_id, change in list(self._deferred_changes.items()):
            if change.source.opposite is not recovered_side:
                continue
            pair = self.store.pairs.get(pair_id)
            if pair is None:
                self._deferred_changes.pop(pair_id, None)
                continue
            target = self._current(recovered_side).get(pair.uid(recovered_side))
            if target is None:
                continue
            self._deferred_changes.pop(pair_id, None)
            await self._propagate_change(
                change.source, pair, change.baseline, change.desired
            )

    async def _handle_replacement_candidates(  # noqa: C901
        self, side: Side, current: Snapshot, added: set[str]
    ) -> set[str]:
        """Quarantine or confirm compatible UIDs near a mapped disappearance."""
        handled: set[str] = set()
        now = time.monotonic()
        for removal in list(self._removal_candidates.values()):
            if removal.side is not side:
                continue
            existing = self._rename_candidates.get((side, removal.pair_id))
            if (
                existing is not None
                and existing.replacement.uid not in current
                and self._generation[side] - existing.first_replacement_generation
                >= SETTLING_MAX_GENERATIONS
            ):
                self._rename_candidates.pop((side, removal.pair_id), None)
                existing = None
                _LOGGER.debug(
                    "Expired missing replacement candidate for pair %s",
                    removal.pair_id,
                )
            compatible = [
                item
                for item in current.values()
                if (
                    item.uid in added
                    or (existing and item.uid == existing.replacement.uid)
                )
                and item.status is removal.previous.status
                and self._pair_for_uid(side, item.uid) is None
            ]
            if len(compatible) != 1:
                if len(compatible) > 1:
                    _LOGGER.warning(
                        "Ambiguous replacement UIDs for pair %s", removal.pair_id
                    )
                    handled.update(item.uid for item in compatible)
                continue
            replacement = compatible[0]
            if replacement.uid in added:
                handled.add(replacement.uid)
            pair = self.store.pairs.get(removal.pair_id)
            if pair is None:
                continue
            if replacement.normalized_summary == removal.previous.normalized_summary:
                pair.set_uid(side, replacement.uid)
                self._removal_candidates.pop((side, removal.uid), None)
                self._recent_items[(side, replacement.uid)] = now
                continue
            key = (side, removal.pair_id)
            rename = self._rename_candidates.get(key)
            if rename is None or rename.replacement.uid != replacement.uid:
                rename = RenameCandidate(
                    side,
                    removal.pair_id,
                    removal.previous,
                    replacement,
                    removal.first_missing_generation,
                    removal.first_missing_monotonic,
                    self._generation[side],
                )
                self._rename_candidates[key] = rename
                _LOGGER.debug(
                    "Rename candidate created for pair %s: %s -> %s",
                    removal.pair_id,
                    removal.uid,
                    replacement.uid,
                )
            stabilized = (
                self._generation[side] > rename.first_missing_generation
                and now - rename.first_missing_monotonic
                >= REMOVAL_STABILIZATION_SECONDS
            )
            if stabilized and (
                rename.first_replacement_generation == rename.first_missing_generation
            ):
                self._rename_candidates.pop(key, None)
                await self._propagate_add(side, replacement)
                _LOGGER.debug(
                    "Simultaneous DELETE/ADD released as independent ADD for %s:%s",
                    side,
                    replacement.uid,
                )
            elif stabilized:
                pair.set_uid(side, replacement.uid)
                self._rename_candidates.pop(key, None)
                self._removal_candidates.pop((side, removal.uid), None)
                self._recent_items[(side, replacement.uid)] = now
                await self._propagate_change(side, pair, removal.previous, replacement)
                _LOGGER.debug("Confirmed replacement rename for pair %s", pair.pair_id)
        return handled

    async def _resolve_unresolved_delete(
        self, side: Side, current: Snapshot
    ) -> set[str]:
        """Delete a reappearing target whose mapped source delete was confirmed."""
        handled: set[str] = set()
        for pair_id, source in list(self._unresolved_deletes.items()):
            if source.opposite is not side:
                continue
            pair = self.store.pairs.get(pair_id)
            if pair is None:
                self._unresolved_deletes.pop(pair_id, None)
                continue
            uid = pair.uid(side)
            target = current.get(uid)
            if target is None:
                continue
            await self.entities[side].async_delete_todo_items([uid])
            self._expect(
                side,
                ExpectedOperation(
                    ExpectedOperationType.DELETE,
                    target,
                    None,
                    frozenset(self.snapshots[side]),
                    pair_id=pair_id,
                ),
            )
            handled.add(uid)
            self.store.pairs.pop(pair_id, None)
            self._unresolved_deletes.pop(pair_id, None)
            for key, candidate in list(self._removal_candidates.items()):
                if candidate.pair_id == pair_id:
                    self._removal_candidates.pop(key)
            _LOGGER.debug("Resolved deferred mapped DELETE for pair %s", pair_id)
        return handled

    async def _process_removals(
        self,
        side: Side,
        previous: Snapshot,
        current: Snapshot,
        removed: set[str],
        seeded: set[str] | None = None,
    ) -> None:
        """Observe omissions and propagate only confirmed mapped deletions."""
        candidate_uids = {
            uid
            for candidate_side, uid in self._removal_candidates
            if candidate_side is side and uid not in current
        } - (seeded or set())
        confirmed: list[tuple[str, ItemPair]] = []
        for uid in removed | candidate_uids:
            pair = self._pair_for_uid(side, uid)
            if pair is None:
                _LOGGER.warning(
                    "Delete not propagated: no reliable target mapping for %s:%s",
                    side,
                    uid,
                )
                continue
            candidate = self._removal_candidates.get((side, uid))
            prior = previous.get(uid) or (candidate.previous if candidate else None)
            if prior is not None and self._observe_removal(side, pair, prior):
                confirmed.append((uid, pair))

        for uid, pair in confirmed:
            if (side, pair.pair_id) in self._rename_candidates:
                continue
            target_uid = pair.uid(side.opposite)
            target_item = self._current(side.opposite).get(target_uid)
            if target_item is None:
                other_source = self._unresolved_deletes.get(pair.pair_id)
                if other_source is not None and other_source is not side:
                    self.store.pairs.pop(pair.pair_id, None)
                    self._unresolved_deletes.pop(pair.pair_id, None)
                    for key, candidate in list(self._removal_candidates.items()):
                        if candidate.pair_id == pair.pair_id:
                            self._removal_candidates.pop(key)
                    _LOGGER.debug(
                        "Retired pair %s after independently confirmed absence",
                        pair.pair_id,
                    )
                else:
                    self._unresolved_deletes[pair.pair_id] = side
                    _LOGGER.warning(
                        "Delete target %s is absent; retaining unresolved mapping",
                        target_uid,
                    )
                continue
            await self.entities[side.opposite].async_delete_todo_items([target_uid])
            self._expect(
                side.opposite,
                ExpectedOperation(
                    ExpectedOperationType.DELETE,
                    target_item,
                    None,
                    frozenset(self.snapshots[side.opposite]),
                    pair_id=pair.pair_id,
                ),
            )
            _LOGGER.debug("Confirmed delete propagated for %s:%s", side, uid)
            self.store.pairs.pop(pair.pair_id, None)
            self._unresolved_deletes.pop(pair.pair_id, None)
            self._removal_candidates.pop((side, uid), None)
            self._recent_items.pop((side, uid), None)

    def _seed_removal_candidates(
        self, side: Side, previous: Snapshot, removed: set[str]
    ) -> set[str]:
        """Create first-observation candidates without counting one callback twice."""
        seeded: set[str] = set()
        now = time.monotonic()
        for uid in removed:
            pair = self._pair_for_uid(side, uid)
            if pair is None or (side, uid) in self._removal_candidates:
                continue
            self._removal_candidates[(side, uid)] = RemovalCandidate(
                side,
                uid,
                pair.pair_id,
                previous[uid],
                self._generation[side],
                now,
            )
            seeded.add(uid)
            _LOGGER.debug(
                "Removal candidate created for %s:%s pair %s at generation %d",
                side,
                uid,
                pair.pair_id,
                self._generation[side],
            )
        return seeded

    async def _apply_delta(
        self, side: Side, previous: Snapshot, current: Snapshot
    ) -> None:
        removed = set(previous) - set(current)
        added = set(current) - set(previous)

        resolved_deletes = await self._resolve_unresolved_delete(side, current)
        added -= resolved_deletes

        handled, returned_changes = self._cancel_or_repair_removal_candidates(
            side, current
        )
        added -= handled
        now = time.monotonic()
        for uid in added:
            self._recent_items[(side, uid)] = now

        seeded = self._seed_removal_candidates(side, previous, removed)
        added -= await self._handle_replacement_candidates(side, current, added)

        await self._process_removals(side, previous, current, removed - seeded, seeded)

        for pair, old, new in returned_changes:
            await self._propagate_change(side, pair, old, new)

        for uid in added:
            await self._propagate_add(side, current[uid])

        for uid in set(previous) & set(current):
            old, new = previous[uid], current[uid]
            if old != new:
                pair = self._pair_for_uid(side, uid)
                if pair is None:
                    await self._propagate_add(side, new)
                else:
                    await self._propagate_change(side, pair, old, new)
        await self._apply_deferred_changes(side)
        await self.store.async_save()

    async def _propagate_add(self, source: Side, item: SyncItem) -> None:
        target = source.opposite
        if self._pair_for_uid(source, item.uid) is not None:
            return
        if any(
            operation.operation is ExpectedOperationType.CREATE
            and operation.source_side is source
            and operation.source_uid == item.uid
            for operation in self._expected[target]
        ):
            _LOGGER.debug(
                "CREATE remains unresolved for %s:%s; not retrying", source, item.uid
            )
            return
        matches = [
            candidate
            for candidate in self._current(target).values()
            if candidate.normalized_summary == item.normalized_summary
        ]
        same_status = [
            candidate for candidate in matches if candidate.status is item.status
        ]
        if same_status:
            matches = same_status
        elif item.status is TodoItemStatus.NEEDS_ACTION:
            active = [
                candidate
                for candidate in matches
                if candidate.status is TodoItemStatus.NEEDS_ACTION
            ]
            if active:
                matches = active
        if len(matches) > 1:
            _LOGGER.warning(
                "Ambiguous duplicate items for normalized summary %r",
                item.normalized_summary,
            )
            return
        if matches and self._pair_for_uid(target, matches[0].uid) is not None:
            _LOGGER.warning(
                "Equivalent target item is already mapped for normalized summary %r",
                item.normalized_summary,
            )
            return
        pair_id = str(uuid4())
        target_item = (
            matches[0]
            if matches
            else await self._create(
                target,
                item,
                source_side=source,
                source_uid=item.uid,
                pair_id=pair_id,
            )
        )
        if target_item is None:
            return
        pair = ItemPair(pair_id, "", "", item.normalized_summary)
        pair.set_uid(source, item.uid)
        pair.set_uid(target, target_item.uid)
        self.store.pairs[pair.pair_id] = pair
        if target_item.status != item.status:
            await self._update(target, target_item.uid, status=item.status)
        _LOGGER.debug(
            "Mapped %s:%s to %s:%s", source, item.uid, target, target_item.uid
        )

    async def _propagate_change(
        self, source: Side, pair: ItemPair, old: SyncItem, new: SyncItem
    ) -> None:
        target = source.opposite
        target_items = self._current(target)
        target_item = target_items.get(pair.uid(target))
        if target_item is None:
            _LOGGER.warning(
                "Mapped target missing during update; deferring pair %s without CREATE",
                pair.pair_id,
            )
            deferred = self._deferred_changes.get(pair.pair_id)
            if deferred is None or deferred.source is not source:
                self._deferred_changes[pair.pair_id] = DeferredChange(source, old, new)
            else:
                deferred.desired = new
            return
        rename = new.summary if old.summary != new.summary else None
        status = new.status if old.status != new.status else None
        if rename is None and status is None:
            return
        # Do not pick a winner if both sides independently renamed since baseline.
        target_old = self.snapshots[target].get(target_item.uid)
        if (
            rename is not None
            and target_old
            and target_old.summary != target_item.summary
        ):
            _LOGGER.warning(
                "Concurrent Todo Sync rename conflict for pair %s", pair.pair_id
            )
            return
        replacement = await self._update(
            target, target_item.uid, summary=rename, status=status
        )
        if replacement:
            pair.set_uid(target, replacement.uid)
            pair.normalized_summary = new.normalized_summary

    async def _create(
        self,
        side: Side,
        item: SyncItem,
        *,
        source_side: Side | None = None,
        source_uid: str | None = None,
        pair_id: str | None = None,
    ) -> SyncItem | None:
        entity = self.entities[side]
        before = self._current(side)
        await entity.async_create_todo_item(
            TodoItem(summary=item.summary, status=item.status)
        )
        after = self._current(side)
        created = [
            value
            for uid, value in after.items()
            if uid not in before and value.normalized_summary == item.normalized_summary
        ]
        result = created[0] if len(created) == 1 else None
        expected_after = result or SyncItem("", item.summary, item.status)
        self._expect(
            side,
            ExpectedOperation(
                ExpectedOperationType.CREATE,
                None,
                expected_after,
                frozenset(before),
                source_side,
                source_uid,
                pair_id,
            ),
        )
        if result is None:
            _LOGGER.warning(
                "Could not identify newly created Todo Sync item unambiguously"
            )
            return None
        self._recent_items[(side, result.uid)] = time.monotonic()
        return result

    async def _update(
        self,
        side: Side,
        uid: str,
        *,
        summary: str | None = None,
        status: TodoItemStatus | None = None,
    ) -> SyncItem | None:
        """Update only shared fields, thereby preserving provider-only metadata."""
        entity = self.entities[side]
        existing = next(
            (item for item in entity.todo_items or () if item.uid == uid), None
        )
        if existing is None:
            _LOGGER.warning("Cannot update missing Todo Sync target %s:%s", side, uid)
            return None
        updated = replace(
            existing,
            summary=summary if summary is not None else existing.summary,
            status=status if status is not None else existing.status,
        )
        await entity.async_update_todo_item(updated)
        after = self._current(side)
        normalized = (
            summary.strip().casefold()
            if summary is not None
            else self.snapshots[side]
            .get(uid, SyncItem(uid, "", TodoItemStatus.NEEDS_ACTION))
            .normalized_summary
        )
        candidates = [
            item for item in after.values() if item.normalized_summary == normalized
        ]
        result = after.get(uid) or (candidates[0] if len(candidates) == 1 else None)
        before = SyncItem.from_todo_item(existing)
        if before is not None:
            expected_after = SyncItem(
                uid,
                summary if summary is not None else before.summary,
                status if status is not None else before.status,
            )
            pair = self._pair_for_uid(side, uid)
            self._expect(
                side,
                ExpectedOperation(
                    ExpectedOperationType.RENAME
                    if summary is not None
                    else ExpectedOperationType.UPDATE_STATUS,
                    before,
                    expected_after,
                    frozenset(self.snapshots[side]),
                    pair_id=pair.pair_id if pair else None,
                ),
            )
        return result

    async def async_reconcile(self) -> None:  # noqa: C901, PLR0912, PLR0915
        """Conservatively validate, repair, and merge complete list snapshots."""
        async with self._lock:
            self._update_availability()
            if not self._both_available():
                return
            current = {Side.A: self._current(Side.A), Side.B: self._current(Side.B)}
            conflicts = 0

            # Existing mappings prove prior identity. Missing means unknown: repair
            # only from a positively observed compatible UID, never via CREATE.
            for pair in self.store.pairs.values():
                for side in (Side.A, Side.B):
                    if pair.uid(side) in current[side]:
                        continue
                    if (side, pair.uid(side)) in self._removal_candidates:
                        conflicts += 1
                        continue
                    counterpart = current[side.opposite].get(pair.uid(side.opposite))
                    candidates = [
                        item
                        for item in current[side].values()
                        if item.normalized_summary == pair.normalized_summary
                        and (counterpart is None or item.status is counterpart.status)
                        and self._pair_for_uid(side, item.uid) is None
                    ]
                    if len(candidates) == 1:
                        pair.set_uid(side, candidates[0].uid)
                        _LOGGER.debug(
                            "Reconciliation repaired missing mapped UID on side %s",
                            side,
                        )
                    elif len(candidates) > 1:
                        conflicts += 1
                        _LOGGER.warning(
                            "Ambiguous UID repair for mapped summary %r on side %s",
                            pair.normalized_summary,
                            side,
                        )
                    else:
                        conflicts += 1
                        _LOGGER.warning(
                            "Mapped UID %s missing on side %s; preserving "
                            "unresolved mapping",
                            pair.uid(side),
                            side,
                        )

            for pair in self.store.pairs.values():
                a_item = current[Side.A].get(pair.a_uid)
                b_item = current[Side.B].get(pair.b_uid)
                if a_item and b_item and a_item.status != b_item.status:
                    if a_item.status is TodoItemStatus.NEEDS_ACTION:
                        await self._update(Side.B, b_item.uid, status=a_item.status)
                    else:
                        await self._update(Side.A, a_item.uid, status=b_item.status)

            mapped = {
                side: {pair.uid(side) for pair in self.store.pairs.values()}
                for side in (Side.A, Side.B)
            }
            grouped: dict[Side, dict[str, list[SyncItem]]] = {}
            for side in (Side.A, Side.B):
                by_summary: dict[str, list[SyncItem]] = defaultdict(list)
                for item in current[side].values():
                    if item.uid not in mapped[side]:
                        by_summary[item.normalized_summary].append(item)
                grouped[side] = by_summary

            ambiguous = {
                normalized
                for side in (Side.A, Side.B)
                for normalized, items in grouped[side].items()
                if len(items) > 1
            }

            for normalized in set(grouped[Side.A]) & set(grouped[Side.B]):
                a_items, b_items = (
                    grouped[Side.A][normalized],
                    grouped[Side.B][normalized],
                )
                if len(a_items) != 1 or len(b_items) != 1:
                    conflicts += 1
                    _LOGGER.warning(
                        "Ambiguous duplicate items for normalized summary %r",
                        normalized,
                    )
                    continue
                a_item, b_item = a_items[0], b_items[0]
                pair = ItemPair(str(uuid4()), a_item.uid, b_item.uid, normalized)
                self.store.pairs[pair.pair_id] = pair
                mapped[Side.A].add(a_item.uid)
                mapped[Side.B].add(b_item.uid)
                if a_item.status != b_item.status:
                    if a_item.status is TodoItemStatus.NEEDS_ACTION:
                        await self._update(Side.B, b_item.uid, status=a_item.status)
                    else:
                        await self._update(Side.A, a_item.uid, status=b_item.status)

            # Merge only unmatched active items; completed history remains in place.
            for source in (Side.A, Side.B):
                for item in current[source].values():
                    if (
                        item.uid in mapped[source]
                        or item.status is not TodoItemStatus.NEEDS_ACTION
                        or item.normalized_summary in ambiguous
                    ):
                        continue
                    target_matches = grouped[source.opposite].get(
                        item.normalized_summary, []
                    )
                    if len(target_matches) > 1:
                        conflicts += 1
                        continue
                    await self._propagate_add(source, item)
                    mapped[source].add(item.uid)

            self.snapshots = {
                Side.A: self._current(Side.A),
                Side.B: self._current(Side.B),
            }
            await self.store.async_save()
            _LOGGER.debug(
                "Reconciliation completed: %d mappings, %d conflicts",
                len(self.store.pairs),
                conflicts,
            )
