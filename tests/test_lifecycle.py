"""Lifecycle, persistence, availability, and registry regression tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.todo import DATA_COMPONENT, TodoItemStatus
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.todo_sync.const import (
    CONF_ENTITY_A_REGISTRY_ID,
    CONF_ENTITY_B_REGISTRY_ID,
    DOMAIN,
)
from custom_components.todo_sync.models import ItemPair, Side
from custom_components.todo_sync.storage import MappingStore
from custom_components.todo_sync.sync import TodoSyncEngine
from tests.test_sync import MemoryTodoEntity, item

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant


class FakeTodoComponent:
    """Small stand-in for Home Assistant's loaded todo EntityComponent."""

    def __init__(self, entities: dict[str, MemoryTodoEntity]) -> None:
        """Initialize an entity lookup."""
        self.entities = entities

    def get_entity(self, entity_id: str) -> MemoryTodoEntity | None:
        """Return the current entity object for an entity ID."""
        return self.entities.get(entity_id)


def _registry_pair(hass: HomeAssistant) -> tuple[er.RegistryEntry, er.RegistryEntry]:
    """Register two todo entities and expose available states."""
    registry = er.async_get(hass)
    a = registry.async_get_or_create("todo", "test", "a", suggested_object_id="a")
    b = registry.async_get_or_create("todo", "test", "b", suggested_object_id="b")
    hass.states.async_set(a.entity_id, "1")
    hass.states.async_set(b.entity_id, "1")
    return a, b


def _entry(a: er.RegistryEntry, b: er.RegistryEntry) -> MockConfigEntry:
    """Build a config entry containing stable registry IDs."""
    return MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_ENTITY_A_REGISTRY_ID: a.id,
            CONF_ENTITY_B_REGISTRY_ID: b.id,
        },
    )


async def test_mapping_store_survives_restart(hass: HomeAssistant) -> None:
    """A fresh MappingStore instance reloads stable pair IDs and provider UIDs."""
    first = MappingStore(hass, "restart-test")
    first.pairs["stable"] = ItemPair("stable", "a1", "b7", "milk")
    await first.async_save()

    restarted = MappingStore(hass, "restart-test")
    await restarted.async_load()

    assert restarted.pairs["stable"] == first.pairs["stable"]


async def test_unavailable_defers_then_recovery_reconciles(
    hass: HomeAssistant,
) -> None:
    """Unavailable never means empty; recovery merges the deferred active item."""
    registry_a, registry_b = _registry_pair(hass)
    a = MemoryTodoEntity([item("a1")])
    b = MemoryTodoEntity([item("b1")])
    hass.data[DATA_COMPONENT] = FakeTodoComponent(
        {registry_a.entity_id: a, registry_b.entity_id: b}
    )
    runtime = TodoSyncEngine(hass, _entry(registry_a, registry_b))
    await runtime.async_start()
    calls_before = list(b.calls)

    hass.states.async_set(registry_b.entity_id, STATE_UNAVAILABLE)
    await hass.async_block_till_done()
    a._attr_todo_items.append(item("a2", "Bread"))
    a.async_update_listeners()
    await hass.async_block_till_done()

    assert b.calls == calls_before
    assert runtime.snapshots[Side.A] == {"a1": runtime.snapshots[Side.A]["a1"]}

    hass.states.async_set(registry_b.entity_id, "1")
    await hass.async_block_till_done()

    assert {todo.summary for todo in b.todo_items or []} == {"Milk", "Bread"}
    await runtime.async_stop()


async def test_entity_registry_rename_rebinds_and_keeps_syncing(
    hass: HomeAssistant,
) -> None:
    """A registry entity-ID rename replaces listeners and preserves operation."""
    registry_a, registry_b = _registry_pair(hass)
    old_a = MemoryTodoEntity([item("a1")])
    new_a = MemoryTodoEntity([item("a1")])
    b = MemoryTodoEntity([item("b1")])
    component = FakeTodoComponent(
        {registry_a.entity_id: old_a, registry_b.entity_id: b}
    )
    hass.data[DATA_COMPONENT] = component
    runtime = TodoSyncEngine(hass, _entry(registry_a, registry_b))
    await runtime.async_start()

    new_entity_id = "todo.renamed_a"
    component.entities[new_entity_id] = new_a
    er.async_get(hass).async_update_entity(
        registry_a.entity_id, new_entity_id=new_entity_id
    )
    hass.states.async_remove(registry_a.entity_id)
    hass.states.async_set(new_entity_id, "1")
    await hass.async_block_till_done()

    assert runtime.entity_ids[Side.A] == new_entity_id
    assert not old_a._update_listeners
    assert new_a._update_listeners

    new_a.todo_items[0].status = TodoItemStatus.COMPLETED
    new_a.async_update_listeners()
    await hass.async_block_till_done()

    assert b.todo_items[0].status is TodoItemStatus.COMPLETED
    await runtime.async_stop()
