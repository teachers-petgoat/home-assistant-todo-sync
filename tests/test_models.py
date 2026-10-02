"""Tests for provider-independent models."""

from homeassistant.components.todo import TodoItem, TodoItemStatus

from custom_components.todo_sync.models import normalize_summary, snapshot


def test_normalize_summary() -> None:
    """Normalization strips and uses Unicode case folding without fuzzy edits."""
    assert normalize_summary("  STRASSE  ") == "strasse"
    assert normalize_summary("  Straße  ") == "strasse"
    assert normalize_summary("whole  milk") == "whole  milk"


def test_snapshot_contains_only_common_fields() -> None:
    """Snapshots intentionally omit metadata outside v1 semantics."""
    result = snapshot(
        [
            TodoItem(
                uid="one",
                summary="Milk",
                status=TodoItemStatus.NEEDS_ACTION,
                description="two litres",
            )
        ]
    )
    assert result["one"].summary == "Milk"
    assert result["one"].normalized_summary == "milk"
    assert not hasattr(result["one"], "description")
