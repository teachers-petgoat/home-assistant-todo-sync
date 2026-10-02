"""Focused runtime synchronization tests using in-memory TodoListEntity objects."""

from __future__ import annotations

from dataclasses import replace

import pytest
from homeassistant.components.todo import TodoItem, TodoItemStatus, TodoListEntity

from custom_components.todo_sync.models import ItemPair, Side, snapshot
from custom_components.todo_sync.sync import TodoSyncEngine


class MemoryTodoEntity(TodoListEntity):
    """Minimal provider that optionally replaces UIDs when renaming."""

    def __init__(self, items: list[TodoItem], *, replace_rename: bool = False) -> None:
        self._attr_todo_items = items
        self.replace_rename = replace_rename
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

    async def async_update_todo_item(self, item: TodoItem) -> None:
        self.calls.append("update")
        old = next(value for value in self._attr_todo_items if value.uid == item.uid)
        uid = (
            str(self._next_uid + 1) if self.replace_rename and item.summary else old.uid
        )
        if uid != old.uid:
            self._next_uid += 1
        self._attr_todo_items[self._attr_todo_items.index(old)] = TodoItem(
            uid=uid,
            summary=item.summary or old.summary,
            status=item.status or old.status,
            description=old.description,
            due=old.due,
        )

    async def async_delete_todo_items(self, uids: list[str]) -> None:
        self.calls.append("delete")
        self._attr_todo_items = [
            item for item in self._attr_todo_items if item.uid not in uids
        ]

    async def async_update_ha_state(self, force_refresh: bool = False) -> None:
        del force_refresh


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
    return TodoItem(uid=uid, summary=summary, status=status, description=description)


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
    return result


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
    if source is Side.A:
        assert target.description == "two litres"


@pytest.mark.parametrize("source", [Side.A, Side.B])
async def test_rename_both_directions_and_replacement_uid(source: Side) -> None:
    """Rename follows mappings and records a target provider's replacement UID."""
    entities = {
        Side.A: MemoryTodoEntity([item("a")]),
        Side.B: MemoryTodoEntity([item("b")], replace_rename=True),
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
    if source is Side.A:
        assert runtime.store.pairs["pair"].b_uid != "b"


@pytest.mark.parametrize("source", [Side.A, Side.B])
async def test_mapped_delete_both_directions(source: Side) -> None:
    """Only the exact mapped target UID is deleted."""
    entities = {
        Side.A: MemoryTodoEntity([item("a")]),
        Side.B: MemoryTodoEntity([item("b")]),
    }
    runtime = engine(
        entities[Side.A], entities[Side.B], [ItemPair("pair", "a", "b", "milk")]
    )
    previous = runtime._current(source)
    entities[source]._attr_todo_items.clear()
    await runtime._apply_delta(source, previous, {})
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
