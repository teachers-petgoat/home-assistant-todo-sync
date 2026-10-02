"""The Todo Sync integration."""

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN
from .sync import SyncSetupError, TodoSyncEngine


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Todo Sync from a config entry."""
    engine = TodoSyncEngine(hass, entry)
    try:
        await engine.async_start()
    except SyncSetupError as err:
        raise ConfigEntryNotReady(str(err)) from err
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = engine
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Todo Sync config entry."""
    engine: TodoSyncEngine | None = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    if engine is not None:
        await engine.async_stop()
    return True
