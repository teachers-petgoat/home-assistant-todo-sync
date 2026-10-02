"""Config flow for Todo Sync."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import voluptuous as vol
from homeassistant.components.todo import DOMAIN as TODO_DOMAIN
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import ATTR_FRIENDLY_NAME, ATTR_SUPPORTED_FEATURES
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.selector import EntitySelector, EntitySelectorConfig

from .const import (
    CONF_ENTITY_A,
    CONF_ENTITY_A_REGISTRY_ID,
    CONF_ENTITY_B,
    CONF_ENTITY_B_REGISTRY_ID,
    DOMAIN,
    REQUIRED_FEATURES,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

ENTITY_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ENTITY_A): EntitySelector(
            EntitySelectorConfig(domain=TODO_DOMAIN)
        ),
        vol.Required(CONF_ENTITY_B): EntitySelector(
            EntitySelectorConfig(domain=TODO_DOMAIN)
        ),
    }
)


def _validate_entity(hass: HomeAssistant, entity_id: str) -> str | None:
    """Return a translation key when an entity cannot support Todo Sync."""
    if not entity_id.startswith(f"{TODO_DOMAIN}."):
        return "not_todo_entity"

    state = hass.states.get(entity_id)
    if state is None:
        return "entity_not_found"

    supported = int(state.attributes.get(ATTR_SUPPORTED_FEATURES, 0))
    if supported & int(REQUIRED_FEATURES) != int(REQUIRED_FEATURES):
        return "missing_capabilities"

    if er.async_get(hass).async_get(entity_id) is None:
        return "entity_not_registered"

    return None


class TodoSyncConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Todo Sync."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Select and validate exactly two todo entities."""
        errors: dict[str, str] = {}

        if user_input is not None:
            entity_a = user_input[CONF_ENTITY_A]
            entity_b = user_input[CONF_ENTITY_B]

            if entity_a == entity_b:
                errors["base"] = "same_entity"
            else:
                for field, entity_id in (
                    (CONF_ENTITY_A, entity_a),
                    (CONF_ENTITY_B, entity_b),
                ):
                    if error := _validate_entity(self.hass, entity_id):
                        errors[field] = error

            if not errors:
                registry = er.async_get(self.hass)
                # Registration was checked during validation; casts narrow the type.
                registry_a = cast("er.RegistryEntry", registry.async_get(entity_a))
                registry_b = cast("er.RegistryEntry", registry.async_get(entity_b))

                pair_id = ":".join(sorted((registry_a.id, registry_b.id)))
                await self.async_set_unique_id(pair_id)
                self._abort_if_unique_id_configured()

                state_a = self.hass.states.get(entity_a)
                state_b = self.hass.states.get(entity_b)
                # Entity existence was checked during validation.
                name_a = state_a.attributes.get(ATTR_FRIENDLY_NAME, entity_a)
                name_b = state_b.attributes.get(ATTR_FRIENDLY_NAME, entity_b)
                return self.async_create_entry(
                    title=f"{name_a} ↔ {name_b}",
                    data={
                        CONF_ENTITY_A: entity_a,
                        CONF_ENTITY_B: entity_b,
                        CONF_ENTITY_A_REGISTRY_ID: registry_a.id,
                        CONF_ENTITY_B_REGISTRY_ID: registry_b.id,
                    },
                )

        return self.async_show_form(
            step_id="user", data_schema=ENTITY_SCHEMA, errors=errors
        )
