"""Tests for the Todo Sync config entry lifecycle."""

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.todo_sync.const import DOMAIN


async def test_setup_and_unload(hass: HomeAssistant) -> None:
    """The scaffold accepts and unloads a config entry."""
    entry = MockConfigEntry(domain=DOMAIN, data={})
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)
    assert entry.state.recoverable
    assert await hass.config_entries.async_unload(entry.entry_id)
