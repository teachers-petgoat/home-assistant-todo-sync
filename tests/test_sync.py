"""Focused runtime synchronization tests using in-memory TodoListEntity objects."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from homeassistant.components.todo import TodoItem, TodoItemStatus, TodoListEntity
from homeassistant.core import HomeAssistant

from custom_components.todo_sync import sync as sync_module
from custom_components.todo_sync.models import ItemPair, Side, snapshot
from custom_components.todo_sync.sync import TodoSyncEngine


class MemoryTodoEntity(TodoListEntity):
    """Minimal provider that optionally replaces UIDs when renaming."""

    def __init__(
        self,
        items: list[TodoItem],
        *,
        replace_rename: bool = False,
        emit_updates: bool = False,
        concurrent_change: TodoItem | None = None,
    ) -> None:
        self._attr_todo_items = items
        self.replace_rename = replace_rename
        self.emit_updates = emit_updates
        self.concurrent_change = concurrent_change
        self.calls: list[str] = []
        self._next_uid = 100

    async def async_create_todo_item(self, item: TodoItem) -> None:
        self._next_uid += 1
        self.calls.append("create")
        self._attr_todo_items.append(
            replace(
                item,
                uid=str(self._next_uid),
                status=item.status or TodoItemStatus.NEEDS_ACTION,
            )
        )
        self._add_concurrent_change()
        if self.emit_updates:
            self.async_update_listeners()

    async def async_update_todo_item(self, item: TodoItem) -> None:
        self.calls.append("update")
        assert item.summary is not None
        old = next(value for value in self._attr_todo_items if value.uid == item.uid)
        uid = (
            str(self._next_uid + 1)
            if self.replace_rename and item.summary != old.summary
            else old.uid
        )
        if uid != old.uid:
            self._next_uid += 1
        self._attr_todo_items[self._attr_todo_items.index(old)] = replace(item, uid=uid)
        self._add_concurrent_change()
        if self.emit_updates:
            self.async_update_listeners()

    async def async_delete_todo_items(self, uids: list[str]) -> None:
        self.calls.append("delete")
        self._attr_todo_items = [
            item for item in self._attr_todo_items if item.uid not in uids
        ]
        self._add_concurrent_change()
        if self.emit_updates:
            self.async_update_listeners()

    async def async_update_ha_state(self, force_refresh: bool = False) -> None:
        del force_refresh

    def _add_concurrent_change(self) -> None:
        if self.concurrent_change is None:
            return
        self._attr_todo_items.append(self.concurrent_change)
        self.concurrent_change = None


class MemoryStore:
    """MappingStore test double."""

    def __init__(self, pairs: list[ItemPair] | None = None) -> None:
        self.pairs = {pair.pair_id: pair for pair in pairs or []}
        self.saves = 0

    async def async_save(self) -> None:
        self.saves += 1


def item(
    uid: str,
    summary: str = "Milk",
    status: TodoItemStatus = TodoItemStatus.NEEDS_ACTION,
    *,
    description: str | None = None,
) -> TodoItem:
    """Build a provider item."""
    return TodoItem(
        uid=uid,
        summary=summary,
        status=status,
        description=description,
        completed=datetime(2026, 1, 1, tzinfo=UTC),
    )


def engine(
    a: MemoryTodoEntity,
    b: MemoryTodoEntity,
    pairs: list[ItemPair] | None = None,
) -> TodoSyncEngine:
    """Build an engine around in-memory entities without HA lifecycle concerns."""
    result = object.__new__(TodoSyncEngine)
    result.entities = {Side.A: a, Side.B: b}
    result.snapshots = {
        Side.A: snapshot(a.todo_items or []),
        Side.B: snapshot(b.todo_items or []),
    }
    result.store = MemoryStore(pairs)
    result._expected = {Side.A: [], Side.B: []}
    result._confirmed = {Side.A: [], Side.B: []}
    result._generation = {Side.A: 0, Side.B: 0}
    result._removal_candidates = {}
    result._recent_items = {}
    return result


def subscribe_engine(runtime: TodoSyncEngine, hass: HomeAssistant) -> None:
    """Attach a directly constructed engine to real HA callback scheduling."""
    runtime.hass = hass
    runtime.entity_ids = {Side.A: "todo.a", Side.B: "todo.b"}
    runtime._unsub_updates = []
    runtime._unsub_state = None
    runtime._unsub_registry = None
    runtime._stopped = False
    runtime._lock = asyncio.Lock()
    runtime._available = {Side.A: True, Side.B: True}
    runtime._update_availability = lambda: None
    runtime._subscribe()


class DelayedTodoEntity(MemoryTodoEntity):
    """Strict provider double whose mutations complete only on cloud delivery."""

    async def async_create_todo_item(self, item: TodoItem) -> None:
        del item
        self.calls.append("create")

    async def async_update_todo_item(self, item: TodoItem) -> None:
        del item
        self.calls.append("update")

    async def async_delete_todo_items(self, uids: list[str]) -> None:
        del uids
        self.calls.append("delete")

    def cloud_snapshot(self, items: list[TodoItem]) -> None:
        """Deliver a complete provider snapshot through the real subscription."""
        self._attr_todo_items = items
        self.async_update_listeners()


@pytest.mark.parametrize("source", [Side.A, Side.B])
async def test_add_both_directions_and_loop_prevention(source: Side) -> None:
    """An add creates one mapped counterpart and replaying it is idempotent."""
    entities = {Side.A: MemoryTodoEntity([]), Side.B: MemoryTodoEntity([])}
    runtime = engine(entities[Side.A], entities[Side.B])
    new = item("source")
    entities[source]._attr_todo_items.append(new)
    current = snapshot(entities[source].todo_items or [])

    await runtime._apply_delta(source, {}, current)
    await runtime._apply_delta(source, current, current)

    assert len(entities[source.opposite].todo_items or []) == 1
    assert len(runtime.store.pairs) == 1
    assert entities[source.opposite].calls == ["create"]


async def test_duplicate_prevention() -> None:
    """An exact normalized target is mapped rather than duplicated."""
    runtime = engine(
        MemoryTodoEntity([item("a", " MILK ")]), MemoryTodoEntity([item("b", "milk")])
    )
    await runtime._apply_delta(Side.A, {}, runtime._current(Side.A))
    assert runtime.entities[Side.B].calls == []
    assert len(runtime.store.pairs) == 1


async def test_real_subscription_payload_never_becomes_empty(
    hass: HomeAssistant,
) -> None:
    """The actual TodoItem callback retains items and cannot imply deletion."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = MemoryTodoEntity([item("b")], emit_updates=True)
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a.todo_items[0].status = TodoItemStatus.COMPLETED
    a.async_update_listeners()
    await hass.async_block_till_done()

    assert set(runtime.snapshots[Side.A]) == {"a"}
    assert b.calls == ["update"]
    assert a.calls == []
    assert "delete" not in b.calls
    for unsubscribe in runtime._unsub_updates:
        unsubscribe()
    runtime._unsub_state()
    runtime._unsub_registry()


@pytest.mark.parametrize("operation", ["update", "create", "delete"])
async def test_concurrent_target_addition_is_not_swallowed(
    hass: HomeAssistant, operation: str
) -> None:
    """Expected echoes suppress only sync mutations, never concurrent additions."""
    initial_items = [] if operation == "create" else [item("a")]
    target_items = [] if operation == "create" else [item("b")]
    a = MemoryTodoEntity(initial_items, emit_updates=True)
    b = MemoryTodoEntity(
        target_items,
        emit_updates=True,
        concurrent_change=item("bread-b", "Bread"),
    )
    pairs = [] if operation == "create" else [ItemPair("pair", "a", "b", "milk")]
    runtime = engine(a, b, pairs)
    subscribe_engine(runtime, hass)

    if operation == "update":
        a.todo_items[0].status = TodoItemStatus.COMPLETED
    elif operation == "create":
        a._attr_todo_items.append(item("a", "Milk"))
    else:
        a._attr_todo_items.clear()
    a.async_update_listeners()
    await hass.async_block_till_done()

    if operation == "delete":
        assert a.todo_items == []
        assert {todo.summary for todo in b.todo_items or []} == {"Milk"}
        assert "pair" in runtime.store.pairs
    else:
        expected = {"Milk", "Bread"}
        assert {todo.summary for todo in a.todo_items or []} == expected
        assert {todo.summary for todo in b.todo_items or []} == expected
    if operation == "update":
        assert all(
            todo.status is TodoItemStatus.COMPLETED
            for entity in (a, b)
            for todo in entity.todo_items or []
            if todo.summary == "Milk"
        )
    assert not runtime._expected[Side.A]
    assert not runtime._expected[Side.B]

    calls = (list(a.calls), list(b.calls))
    await hass.async_block_till_done()
    assert (a.calls, b.calls) == calls
    for unsubscribe in runtime._unsub_updates:
        unsubscribe()
    runtime._unsub_state()
    runtime._unsub_registry()


@pytest.mark.parametrize("source", [Side.A, Side.B])
@pytest.mark.parametrize(
    ("before", "after"),
    [
        (TodoItemStatus.NEEDS_ACTION, TodoItemStatus.COMPLETED),
        (TodoItemStatus.COMPLETED, TodoItemStatus.NEEDS_ACTION),
    ],
)
async def test_complete_and_reopen_both_directions(
    source: Side, before: TodoItemStatus, after: TodoItemStatus
) -> None:
    """Status changes flow in either direction and preserve target metadata."""
    entities = {
        Side.A: MemoryTodoEntity([item("a", status=before)]),
        Side.B: MemoryTodoEntity([item("b", status=before, description="two litres")]),
    }
    pair = ItemPair("pair", "a", "b", "milk")
    runtime = engine(entities[Side.A], entities[Side.B], [pair])
    previous = runtime._current(source)
    changed = next(iter(entities[source].todo_items or []))
    changed.status = after

    await runtime._apply_delta(source, previous, runtime._current(source))

    target = next(iter(entities[source.opposite].todo_items or []))
    assert target.status is after
    assert target.summary == "Milk"
    assert target.completed == datetime(2026, 1, 1, tzinfo=UTC)
    if source is Side.A:
        assert target.description == "two litres"


@pytest.mark.parametrize("source", [Side.A, Side.B])
async def test_rename_both_directions_and_replacement_uid(source: Side) -> None:
    """Rename follows mappings and records a target provider's replacement UID."""
    entities = {
        Side.A: MemoryTodoEntity([item("a", description="A metadata")]),
        Side.B: MemoryTodoEntity(
            [item("b", description="B metadata")], replace_rename=True
        ),
    }
    runtime = engine(
        entities[Side.A], entities[Side.B], [ItemPair("pair", "a", "b", "milk")]
    )
    previous = runtime._current(source)
    next(iter(entities[source].todo_items or [])).summary = "Whole milk"

    await runtime._apply_delta(source, previous, runtime._current(source))

    assert (
        next(iter(entities[source.opposite].todo_items or [])).summary == "Whole milk"
    )
    target = next(iter(entities[source.opposite].todo_items or []))
    assert target.status is TodoItemStatus.NEEDS_ACTION
    assert target.completed == datetime(2026, 1, 1, tzinfo=UTC)
    assert target.description == f"{source.opposite.value.upper()} metadata"
    if source is Side.A:
        assert runtime.store.pairs["pair"].b_uid != "b"


@pytest.mark.parametrize("source", [Side.A, Side.B])
async def test_mapped_delete_both_directions(
    source: Side, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the exact mapped target UID is deleted."""
    entities = {
        Side.A: MemoryTodoEntity([item("a")]),
        Side.B: MemoryTodoEntity([item("b")]),
    }
    runtime = engine(
        entities[Side.A], entities[Side.B], [ItemPair("pair", "a", "b", "milk")]
    )
    clock = 0.0
    monkeypatch.setattr(sync_module.time, "monotonic", lambda: clock)
    previous = runtime._current(source)
    entities[source]._attr_todo_items.clear()
    await runtime._apply_delta(source, previous, {})
    assert entities[source.opposite].todo_items != []
    clock = 6.0
    await runtime._apply_delta(source, {}, {})
    assert entities[source.opposite].todo_items == []
    assert runtime.store.pairs == {}


async def test_unmapped_delete_is_not_propagated() -> None:
    """A same-name item is not enough evidence for destructive propagation."""
    runtime = engine(MemoryTodoEntity([]), MemoryTodoEntity([item("b")]))
    await runtime._apply_delta(Side.A, {"gone": snapshot([item("gone")])["gone"]}, {})
    assert [value.uid for value in runtime.entities[Side.B].todo_items or []] == ["b"]


async def test_reconciliation_merges_active_and_active_wins() -> None:
    """Startup merges unmatched active items and reopens an exact completed match."""
    a = MemoryTodoEntity([item("a1", "Milk"), item("a2", "Bread")])
    b = MemoryTodoEntity(
        [item("b1", "milk", TodoItemStatus.COMPLETED), item("b2", "Butter")]
    )
    runtime = engine(a, b)
    runtime._lock = __import__("asyncio").Lock()
    runtime._available = {Side.A: True, Side.B: True}
    runtime._update_availability = lambda: None
    await runtime.async_reconcile()
    assert {value.summary for value in a.todo_items or []} == {
        "Milk",
        "Bread",
        "Butter",
    }
    assert {value.summary for value in b.todo_items or []} == {
        "milk",
        "Bread",
        "Butter",
    }
    assert (
        next(value for value in b.todo_items or [] if value.uid == "b1").status
        is TodoItemStatus.NEEDS_ACTION
    )


async def test_reconciliation_leaves_ambiguous_duplicates_alone() -> None:
    """Pre-existing duplicate ambiguity is neither mapped nor deleted."""
    a = MemoryTodoEntity([item("a1"), item("a2")])
    b = MemoryTodoEntity([item("b1")])
    runtime = engine(a, b)
    runtime._lock = __import__("asyncio").Lock()
    runtime._available = {Side.A: True, Side.B: True}
    runtime._update_availability = lambda: None
    await runtime.async_reconcile()
    assert len(a.todo_items or []) == 2
    assert len(b.todo_items or []) == 1
    assert runtime.store.pairs == {}


async def test_reconciliation_repairs_stale_mapping_uid() -> None:
    """An exact unique item repairs a provider UID changed during downtime."""
    a = MemoryTodoEntity([item("new-a")])
    b = MemoryTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("stable", "stale-a", "b", "milk")])
    runtime._lock = asyncio.Lock()
    runtime._available = {Side.A: True, Side.B: True}
    runtime._update_availability = lambda: None

    await runtime.async_reconcile()

    assert runtime.store.pairs["stable"].a_uid == "new-a"
    assert a.calls == []
    assert b.calls == []


async def test_delayed_create_masks_old_snapshots_and_adopts_uid(
    hass: HomeAssistant,
) -> None:
    """Repeated pre-create callbacks cannot invert a successful CREATE."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a", "Bread"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    b.cloud_snapshot([])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("cloud-b", "Bread")])
    await hass.async_block_till_done()

    assert b.calls == ["create"]
    assert a.calls == []
    assert next(iter(runtime.store.pairs.values())).b_uid == "cloud-b"
    assert len(b.todo_items or []) == 1


async def test_delayed_delete_is_not_recreated(hass: HomeAssistant) -> None:
    """One source omission is quarantined rather than recreated or deleted."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a._attr_todo_items.clear()
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    await hass.async_block_till_done()

    assert b.calls == []
    assert a.calls == []
    assert not a.todo_items
    assert "pair" in runtime.store.pairs


async def test_delayed_status_does_not_propagate_inverse(
    hass: HomeAssistant,
) -> None:
    """An old-status callback before confirmation is operation-local stale data."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a.todo_items[0].status = TodoItemStatus.COMPLETED
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b", status=TodoItemStatus.COMPLETED)])
    await hass.async_block_till_done()

    assert b.calls == ["update"]
    assert a.calls == []
    assert a.todo_items[0].status is TodoItemStatus.COMPLETED


async def test_delayed_rename_adopts_replacement_uid(
    hass: HomeAssistant,
) -> None:
    """A stale old representation cannot start a delete/create rename loop."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a.todo_items[0].summary = "Whole milk"
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("replacement", "Whole milk")])
    await hass.async_block_till_done()

    assert b.calls == ["update"]
    assert a.calls == []
    assert runtime.store.pairs["pair"].b_uid == "replacement"


async def test_unresolved_create_is_never_retried(hass: HomeAssistant) -> None:
    """A sent CREATE remains protected until a later snapshot supplies its UID."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    await runtime.async_reconcile()
    b.cloud_snapshot([item("later")])
    await hass.async_block_till_done()

    assert b.calls == ["create"]
    assert next(iter(runtime.store.pairs.values())).b_uid == "later"


async def test_ambiguous_delayed_create_stays_unresolved(
    hass: HomeAssistant,
) -> None:
    """Multiple possible created UIDs cause neither retry nor destructive action."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b1"), item("b2")])
    await hass.async_block_till_done()
    await runtime.async_reconcile()

    assert b.calls == ["create"]
    assert a.calls == []
    assert runtime.store.pairs == {}


async def test_stale_create_snapshot_keeps_unrelated_addition(
    hass: HomeAssistant,
) -> None:
    """Masking is UID-local and still propagates another user's addition."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a", "Milk"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("bread-b", "Bread")])
    await hass.async_block_till_done()

    assert {value.summary for value in a.todo_items or []} == {"Milk", "Bread"}
    assert b.calls == ["create"]


async def test_alternating_stale_create_snapshots_remain_bounded(
    hass: HomeAssistant,
) -> None:
    """Even post-confirmation stale callbacks cannot cause cloud ping-pong."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a", "Bread"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b", "Bread")])
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b", "Bread")])
    await hass.async_block_till_done()

    assert b.calls == ["create"]
    assert a.calls == []
    assert len(runtime.store.pairs) == 1


async def test_settled_create_allows_later_user_delete(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CREATE guard retires before a later genuine target-side DELETE."""
    clock = 0.0
    monkeypatch.setattr(sync_module.time, "monotonic", lambda: clock)
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    assert not runtime._confirmed[Side.B]

    b.cloud_snapshot([])
    await hass.async_block_till_done()
    clock = sync_module.REMOVAL_STABILIZATION_SECONDS
    b.cloud_snapshot([])
    await hass.async_block_till_done()

    assert not a.todo_items
    assert a.calls == ["delete"]
    assert b.calls == ["create"]


async def test_settled_status_allows_later_user_reopen(
    hass: HomeAssistant,
) -> None:
    """A retired status guard cannot swallow a later genuine inverse update."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a.todo_items[0].status = TodoItemStatus.COMPLETED
    a.async_update_listeners()
    await hass.async_block_till_done()
    completed = [item("b", status=TodoItemStatus.COMPLETED)]
    b.cloud_snapshot(completed)
    await hass.async_block_till_done()
    b.cloud_snapshot(completed)
    await hass.async_block_till_done()

    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()

    assert a.todo_items[0].status is TodoItemStatus.NEEDS_ACTION
    assert a.calls == ["update"]
    assert b.calls == ["update"]


async def test_settled_rename_allows_later_rename_back(
    hass: HomeAssistant,
) -> None:
    """A retired rename guard cannot hide a user restoring the old summary."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a.todo_items[0].summary = "Whole milk"
    a.async_update_listeners()
    await hass.async_block_till_done()
    renamed = [item("b", "Whole milk")]
    b.cloud_snapshot(renamed)
    await hass.async_block_till_done()
    b.cloud_snapshot(renamed)
    await hass.async_block_till_done()

    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()

    assert a.todo_items[0].summary == "Milk"
    assert a.calls == ["update"]
    assert b.calls == ["update"]


async def test_settled_delete_allows_later_genuine_readd(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A new same-summary item after a settled DELETE is synchronized as an ADD."""
    clock = 0.0
    monkeypatch.setattr(sync_module.time, "monotonic", lambda: clock)
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a._attr_todo_items.clear()
    a.async_update_listeners()
    await hass.async_block_till_done()
    clock = sync_module.REMOVAL_STABILIZATION_SECONDS
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    await hass.async_block_till_done()

    b.cloud_snapshot([item("new-b")])
    await hass.async_block_till_done()

    assert len(a.todo_items or []) == 1
    assert a.calls == ["create"]
    assert b.calls == ["delete"]


async def test_confirmed_guards_are_retired_after_many_settled_operations(
    hass: HomeAssistant,
) -> None:
    """Settled internal operations never accumulate as historical masks."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    for index in range(8):
        status = (
            TodoItemStatus.COMPLETED if index % 2 == 0 else TodoItemStatus.NEEDS_ACTION
        )
        a.todo_items[0].status = status
        a.async_update_listeners()
        await hass.async_block_till_done()
        settled = [item("b", status=status)]
        b.cloud_snapshot(settled)
        await hass.async_block_till_done()
        b.cloud_snapshot(settled)
        await hass.async_block_till_done()

    assert len(b.calls) == 8
    assert not runtime._expected[Side.B]
    assert not runtime._confirmed[Side.B]


async def test_single_stale_omission_after_create_returns_without_delete(
    hass: HomeAssistant,
) -> None:
    """A newly created cloud item may vanish once without reversing CREATE."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()

    assert a.calls == []
    assert b.calls == ["create"]
    assert runtime.store.pairs[next(iter(runtime.store.pairs))].b_uid == "b"
    assert len(a.todo_items or []) == len(b.todo_items or []) == 1


async def test_rapid_repeated_omissions_do_not_confirm_delete(
    hass: HomeAssistant,
) -> None:
    """Rapid omissions of a newly created target remain non-destructive."""
    a = MemoryTodoEntity([], emit_updates=True)
    b = DelayedTodoEntity([])
    runtime = engine(a, b)
    subscribe_engine(runtime, hass)

    a._attr_todo_items.append(item("a"))
    a.async_update_listeners()
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()
    b.cloud_snapshot([])
    b.cloud_snapshot([])
    await hass.async_block_till_done()
    b.cloud_snapshot([item("b")])
    await hass.async_block_till_done()

    assert a.calls == []
    assert b.calls == ["create"]
    assert next(iter(runtime.store.pairs.values())).b_uid == "b"
    assert runtime._removal_candidates == {}


async def test_confirmed_user_delete_requires_later_callback(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stable mapped deletion propagates once after timed confirmation."""
    clock = 100.0
    monkeypatch.setattr(sync_module.time, "monotonic", lambda: clock)
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a._attr_todo_items.clear()
    a.async_update_listeners()
    await hass.async_block_till_done()
    assert b.calls == []
    clock += sync_module.REMOVAL_STABILIZATION_SECONDS
    a.async_update_listeners()
    await hass.async_block_till_done()

    assert b.calls == ["delete"]
    assert runtime.store.pairs == {}


async def test_one_missing_observation_remains_pending(hass: HomeAssistant) -> None:
    """No follow-up callback means no destructive convergence attempt."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = MemoryTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a._attr_todo_items.clear()
    a.async_update_listeners()
    await hass.async_block_till_done()

    assert b.calls == []
    assert "pair" in runtime.store.pairs
    assert (Side.A, "a") in runtime._removal_candidates


async def test_pending_removal_does_not_block_unrelated_add(
    hass: HomeAssistant,
) -> None:
    """A quarantined omission is local to its UID while another ADD syncs."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = MemoryTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a._attr_todo_items[:] = [item("bread-a", "Bread")]
    a.async_update_listeners()
    await hass.async_block_till_done()

    assert "pair" in runtime.store.pairs
    assert {value.summary for value in b.todo_items or []} == {"Milk", "Bread"}
    assert b.calls == ["create"]


async def test_removal_candidate_replacement_uid_repairs_mapping(
    hass: HomeAssistant,
) -> None:
    """An unambiguous same-summary replacement repairs identity without writes."""
    a = MemoryTodoEntity([item("a")], emit_updates=True)
    b = MemoryTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    a._attr_todo_items.clear()
    a.async_update_listeners()
    await hass.async_block_till_done()
    a._attr_todo_items.append(item("replacement-a"))
    a.async_update_listeners()
    await hass.async_block_till_done()

    assert runtime.store.pairs["pair"].a_uid == "replacement-a"
    assert a.calls == b.calls == []
    assert runtime._removal_candidates == {}


async def test_long_inconsistency_sequence_has_no_destructive_mutations(
    hass: HomeAssistant,
) -> None:
    """Alternating presence inside stabilization never deletes or recreates."""
    a = MemoryTodoEntity([item("a")])
    b = DelayedTodoEntity([item("b")])
    runtime = engine(a, b, [ItemPair("pair", "a", "b", "milk")])
    subscribe_engine(runtime, hass)

    for values in ([], [], [item("b")], [], [item("b")]):
        b.cloud_snapshot(values)
        await hass.async_block_till_done()

    assert a.calls == []
    assert b.calls == []
    assert len(runtime.store.pairs) == 1
