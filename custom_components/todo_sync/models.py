"""Internal, provider-independent Todo Sync models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from homeassistant.components.todo import TodoItem, TodoItemStatus

if TYPE_CHECKING:
    from collections.abc import Iterable


def normalize_summary(summary: str) -> str:
    """Return the exact-match key used by Todo Sync."""
    return summary.strip().casefold()


@dataclass(frozen=True, slots=True)
class SyncItem:
    """The deliberately small common representation of a todo item."""

    uid: str
    summary: str
    status: TodoItemStatus

    @property
    def normalized_summary(self) -> str:
        """Return the matching key without changing the displayed summary."""
        return normalize_summary(self.summary)

    @classmethod
    def from_todo_item(cls, item: TodoItem) -> SyncItem | None:
        """Convert a Home Assistant item, ignoring malformed provider data."""
        if item.uid is None or item.summary is None or item.status is None:
            return None
        return cls(item.uid, item.summary, item.status)

    @classmethod
    def from_json(cls, value: Any) -> SyncItem | None:
        """Convert the serialized item supplied by async_subscribe_updates."""
        if not isinstance(value, dict):
            return None
        uid, summary, status = (
            value.get("uid"),
            value.get("summary"),
            value.get("status"),
        )
        if not isinstance(uid, str) or not isinstance(summary, str):
            return None
        try:
            parsed_status = TodoItemStatus(status)
        except (TypeError, ValueError):
            return None
        return cls(uid, summary, parsed_status)


Snapshot = dict[str, SyncItem]


def snapshot(items: Iterable[TodoItem]) -> Snapshot:
    """Create an immutable-value snapshot indexed by provider UID."""
    converted = (SyncItem.from_todo_item(item) for item in items)
    return {item.uid: item for item in converted if item is not None}


def snapshot_json(items: Iterable[Any]) -> Snapshot:
    """Create a snapshot from an update subscription payload."""
    converted = (SyncItem.from_json(item) for item in items)
    return {item.uid: item for item in converted if item is not None}


class Side(StrEnum):
    """One side of a synchronization pair."""

    A = "a"
    B = "b"

    @property
    def opposite(self) -> Side:
        """Return the other side."""
        return Side.B if self is Side.A else Side.A


@dataclass(slots=True)
class ItemPair:
    """Persistent association of provider-local identifiers."""

    pair_id: str
    a_uid: str
    b_uid: str
    normalized_summary: str

    def uid(self, side: Side) -> str:
        """Return the UID for a side."""
        return self.a_uid if side is Side.A else self.b_uid

    def set_uid(self, side: Side, uid: str) -> None:
        """Replace a provider UID while preserving pair identity."""
        if side is Side.A:
            self.a_uid = uid
        else:
            self.b_uid = uid
