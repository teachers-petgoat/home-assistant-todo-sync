"""Versioned persistent mapping storage."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.helpers.storage import Store

from .const import DOMAIN
from .models import ItemPair

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

STORAGE_VERSION = 1
STORAGE_MINOR_VERSION = 1


class MappingStore:
    """Persist the minimum data needed to safely identify counterparts."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        """Initialize an entry-specific Home Assistant Store."""
        self._store = Store[dict[str, Any]](
            hass,
            STORAGE_VERSION,
            f"{DOMAIN}.{entry_id}",
            minor_version=STORAGE_MINOR_VERSION,
        )
        self.pairs: dict[str, ItemPair] = {}

    async def async_load(self) -> None:
        """Load mappings; incompatible or malformed rows are ignored safely."""
        data = await self._store.async_load() or {}
        raw_pairs = data.get("pairs", {})
        if not isinstance(raw_pairs, dict):
            return
        for pair_id, raw in raw_pairs.items():
            if not isinstance(pair_id, str) or not isinstance(raw, dict):
                continue
            try:
                pair = ItemPair(
                    pair_id,
                    str(raw["a_uid"]),
                    str(raw["b_uid"]),
                    str(raw["normalized_summary"]),
                )
            except KeyError:
                continue
            self.pairs[pair_id] = pair

    async def async_save(self) -> None:
        """Persist all mappings atomically through Home Assistant."""
        await self._store.async_save(
            {
                "schema_version": STORAGE_VERSION,
                "pairs": {
                    pair_id: {
                        "a_uid": pair.a_uid,
                        "b_uid": pair.b_uid,
                        "normalized_summary": pair.normalized_summary,
                    }
                    for pair_id, pair in self.pairs.items()
                },
            }
        )
