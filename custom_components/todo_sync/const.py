"""Constants for Todo Sync."""

from typing import Final

from homeassistant.components.todo import TodoListEntityFeature

DOMAIN: Final = "todo_sync"

CONF_ENTITY_A: Final = "entity_a"
CONF_ENTITY_B: Final = "entity_b"
CONF_ENTITY_A_REGISTRY_ID: Final = "entity_a_registry_id"
CONF_ENTITY_B_REGISTRY_ID: Final = "entity_b_registry_id"

REQUIRED_FEATURES: Final = (
    TodoListEntityFeature.CREATE_TODO_ITEM
    | TodoListEntityFeature.UPDATE_TODO_ITEM
    | TodoListEntityFeature.DELETE_TODO_ITEM
)
