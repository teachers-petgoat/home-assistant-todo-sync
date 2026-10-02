"""Tests for the Todo Sync config flow."""

from typing import Any

import pytest
from homeassistant.components.todo import TodoListEntityFeature
from homeassistant.config_entries import SOURCE_USER
from homeassistant.const import ATTR_FRIENDLY_NAME, ATTR_SUPPORTED_FEATURES
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType, InvalidData
from homeassistant.helpers import entity_registry as er

from custom_components.todo_sync.const import (
    CONF_ENTITY_A,
    CONF_ENTITY_A_REGISTRY_ID,
    CONF_ENTITY_B,
    CONF_ENTITY_B_REGISTRY_ID,
    DOMAIN,
    REQUIRED_FEATURES,
)


def _add_entity(
    hass: HomeAssistant,
    entity_id: str,
    *,
    features: TodoListEntityFeature = REQUIRED_FEATURES,
    registered: bool = True,
    name: str | None = None,
) -> er.RegistryEntry | None:
    """Add a todo state and, normally, an entity registry entry."""
    registry_entry = None
    if registered:
        registry_entry = er.async_get(hass).async_get_or_create(
            "todo",
            "test",
            entity_id.split(".", 1)[1],
            suggested_object_id=entity_id.split(".", 1)[1],
        )
    hass.states.async_set(
        entity_id,
        "0",
        {
            ATTR_SUPPORTED_FEATURES: int(features),
            ATTR_FRIENDLY_NAME: name or entity_id,
        },
    )
    return registry_entry


async def _submit(hass: HomeAssistant, data: dict[str, Any]) -> dict[str, Any]:
    """Start and submit the user flow."""
    initial = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    assert initial["type"] is FlowResultType.FORM
    assert initial["step_id"] == "user"
    return await hass.config_entries.flow.async_configure(initial["flow_id"], data)


async def test_form_and_success(hass: HomeAssistant) -> None:
    """A pair of fully capable todo entities can be configured."""
    registry_a = _add_entity(hass, "todo.groceries", name="Groceries")
    registry_b = _add_entity(hass, "todo.household", name="Household")

    result = await _submit(
        hass, {CONF_ENTITY_A: "todo.groceries", CONF_ENTITY_B: "todo.household"}
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "Groceries ↔ Household"
    assert result["data"] == {
        CONF_ENTITY_A: "todo.groceries",
        CONF_ENTITY_B: "todo.household",
        CONF_ENTITY_A_REGISTRY_ID: registry_a.id,
        CONF_ENTITY_B_REGISTRY_ID: registry_b.id,
    }
    assert len(hass.config_entries.async_entries(DOMAIN)) == 1


async def test_same_entity_is_rejected(hass: HomeAssistant) -> None:
    """The same entity cannot occupy both sides."""
    _add_entity(hass, "todo.groceries")
    result = await _submit(
        hass, {CONF_ENTITY_A: "todo.groceries", CONF_ENTITY_B: "todo.groceries"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "same_entity"}


@pytest.mark.parametrize(
    ("features", "expected"),
    [
        (
            REQUIRED_FEATURES ^ TodoListEntityFeature.CREATE_TODO_ITEM,
            "missing_capabilities",
        ),
        (
            REQUIRED_FEATURES ^ TodoListEntityFeature.UPDATE_TODO_ITEM,
            "missing_capabilities",
        ),
        (
            REQUIRED_FEATURES ^ TodoListEntityFeature.DELETE_TODO_ITEM,
            "missing_capabilities",
        ),
        (TodoListEntityFeature(0), "missing_capabilities"),
    ],
)
async def test_missing_capabilities_are_rejected(
    hass: HomeAssistant,
    features: TodoListEntityFeature,
    expected: str,
) -> None:
    """Every required feature must be advertised."""
    _add_entity(hass, "todo.capable")
    _add_entity(hass, "todo.limited", features=features)
    result = await _submit(
        hass, {CONF_ENTITY_A: "todo.capable", CONF_ENTITY_B: "todo.limited"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_ENTITY_B: expected}


@pytest.mark.parametrize(
    ("entity_id", "registered", "error"),
    [
        ("sensor.not_a_todo", True, "not_todo_entity"),
        ("todo.missing", True, "entity_not_found"),
        ("todo.unregistered", False, "entity_not_registered"),
    ],
)
async def test_invalid_entities_are_rejected(
    hass: HomeAssistant, entity_id: str, registered: bool, error: str
) -> None:
    """Wrong-domain, missing, and unregistered entities are rejected clearly."""
    _add_entity(hass, "todo.capable")
    if entity_id != "todo.missing":
        _add_entity(hass, entity_id, registered=registered)
    if entity_id.startswith("sensor."):
        with pytest.raises(InvalidData):
            await _submit(
                hass, {CONF_ENTITY_A: "todo.capable", CONF_ENTITY_B: entity_id}
            )
        return
    result = await _submit(
        hass, {CONF_ENTITY_A: "todo.capable", CONF_ENTITY_B: entity_id}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_ENTITY_B: error}


async def test_both_entity_errors_are_reported(hass: HomeAssistant) -> None:
    """Validation reports an error beside each invalid selection."""
    result = await _submit(hass, {CONF_ENTITY_A: "todo.one", CONF_ENTITY_B: "todo.two"})
    assert result["errors"] == {
        CONF_ENTITY_A: "entity_not_found",
        CONF_ENTITY_B: "entity_not_found",
    }


async def test_reversed_duplicate_pair_is_rejected(hass: HomeAssistant) -> None:
    """A configured pair cannot be added again in reverse order."""
    _add_entity(hass, "todo.one")
    _add_entity(hass, "todo.two")
    first = await _submit(hass, {CONF_ENTITY_A: "todo.one", CONF_ENTITY_B: "todo.two"})
    assert first["type"] is FlowResultType.CREATE_ENTRY

    duplicate = await _submit(
        hass, {CONF_ENTITY_A: "todo.two", CONF_ENTITY_B: "todo.one"}
    )
    assert duplicate["type"] is FlowResultType.ABORT
    assert duplicate["reason"] == "already_configured"
