"""Bidirectional snapshot-based synchronization engine."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import replace
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


class SyncSetupError(RuntimeError):
    """Raised when configured entity-registry references cannot be resolved."""


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
            previous = self.snapshots[side]
            if current == previous:
                return
            await self._apply_delta(side, previous, current)
            self.snapshots[side] = current
            self.snapshots[side.opposite] = self._current(side.opposite)

    def _pair_for_uid(self, side: Side, uid: str) -> ItemPair | None:
        return next(
            (pair for pair in self.store.pairs.values() if pair.uid(side) == uid), None
        )

    async def _apply_delta(
        self, side: Side, previous: Snapshot, current: Snapshot
    ) -> None:
        removed = set(previous) - set(current)
        added = set(current) - set(previous)

        # A common provider rename implementation is one removal plus one creation.
        if len(removed) == len(added) == 1:
            old_uid, new_uid = next(iter(removed)), next(iter(added))
            pair = self._pair_for_uid(side, old_uid)
            old, new = previous[old_uid], current[new_uid]
            if pair and old.status == new.status:
                pair.set_uid(side, new_uid)
                await self._propagate_change(side, pair, old, new)
                await self.store.async_save()
                return

        for uid in removed:
            pair = self._pair_for_uid(side, uid)
            if pair is None:
                _LOGGER.warning(
                    "Delete not propagated: no reliable target mapping for %s:%s",
                    side,
                    uid,
                )
                continue
            target_uid = pair.uid(side.opposite)
            if target_uid not in self._current(side.opposite):
                _LOGGER.warning(
                    "Delete target %s no longer exists; removing stale mapping",
                    target_uid,
                )
            else:
                await self.entities[side.opposite].async_delete_todo_items([target_uid])
                _LOGGER.debug("Propagated DELETE %s:%s", side, uid)
            self.store.pairs.pop(pair.pair_id, None)

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
        await self.store.async_save()

    async def _propagate_add(self, source: Side, item: SyncItem) -> None:
        target = source.opposite
        matches = [
            candidate
            for candidate in self._current(target).values()
            if candidate.normalized_summary == item.normalized_summary
        ]
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
        target_item = matches[0] if matches else await self._create(target, item)
        if target_item is None:
            return
        pair = ItemPair(str(uuid4()), "", "", item.normalized_summary)
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
                "Mapped target missing during update; repairing conservatively"
            )
            replacement = await self._create(target, new)
            if replacement:
                pair.set_uid(target, replacement.uid)
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

    async def _create(self, side: Side, item: SyncItem) -> SyncItem | None:
        entity = self.entities[side]
        before = self._current(side)
        await entity.async_create_todo_item(
            TodoItem(summary=item.summary, status=item.status)
        )
        await entity.async_update_ha_state(force_refresh=True)
        after = self._current(side)
        created = [
            value
            for uid, value in after.items()
            if uid not in before and value.normalized_summary == item.normalized_summary
        ]
        if len(created) != 1:
            _LOGGER.warning(
                "Could not identify newly created Todo Sync item unambiguously"
            )
            return None
        return created[0]

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
        await entity.async_update_ha_state(force_refresh=True)
        after = self._current(side)
        if uid in after:
            return after[uid]
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
        return candidates[0] if len(candidates) == 1 else None

    async def async_reconcile(self) -> None:  # noqa: C901, PLR0912, PLR0915
        """Conservatively validate, repair, and merge complete list snapshots."""
        async with self._lock:
            self._update_availability()
            if not self._both_available():
                return
            current = {Side.A: self._current(Side.A), Side.B: self._current(Side.B)}
            conflicts = 0

            # Repair mappings by their last exact summary; never delete during startup.
            for pair_id, pair in list(self.store.pairs.items()):
                for side in (Side.A, Side.B):
                    if pair.uid(side) in current[side]:
                        continue
                    candidates = [
                        item
                        for item in current[side].values()
                        if item.normalized_summary == pair.normalized_summary
                    ]
                    if len(candidates) == 1:
                        pair.set_uid(side, candidates[0].uid)
                    elif len(candidates) > 1:
                        conflicts += 1
                    else:
                        counterpart = current[side.opposite].get(
                            pair.uid(side.opposite)
                        )
                        if (
                            counterpart
                            and counterpart.status is TodoItemStatus.NEEDS_ACTION
                        ):
                            created = await self._create(side, counterpart)
                            if created:
                                pair.set_uid(side, created.uid)
                                current[side] = self._current(side)
                if not any(
                    pair.uid(side) in current[side] for side in (Side.A, Side.B)
                ):
                    self.store.pairs.pop(pair_id)

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
