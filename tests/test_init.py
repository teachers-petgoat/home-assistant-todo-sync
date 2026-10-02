"""Tests for the Todo Sync config entry lifecycle."""

from unittest.mock import AsyncMock, patch

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.todo_sync.const import DOMAIN


async def test_setup_and_unload(hass: HomeAssistant) -> None:
    """Entry setup starts and unload stops the runtime engine."""
    entry = MockConfigEntry(domain=DOMAIN, data={})
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.todo_sync.TodoSyncEngine.async_start",
            new_callable=AsyncMock,
        ) as start,
        patch(
            "custom_components.todo_sync.TodoSyncEngine.async_stop",
            new_callable=AsyncMock,
        ) as stop,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        assert entry.state.recoverable
        start.assert_awaited_once()
        assert await hass.config_entries.async_unload(entry.entry_id)
        stop.assert_awaited_once()
