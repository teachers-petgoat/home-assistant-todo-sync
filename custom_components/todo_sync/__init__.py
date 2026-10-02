"""The Todo Sync integration.

The synchronization runtime will be added separately. This module deliberately only
owns the config-entry lifecycle for the initial scaffold.
"""

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Todo Sync from a config entry."""
    del hass, entry
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Todo Sync config entry."""
    del hass, entry
    return True
