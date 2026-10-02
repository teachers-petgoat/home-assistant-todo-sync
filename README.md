# Todo Sync

Todo Sync is a Home Assistant custom integration intended to synchronize two
Home Assistant `todo` entities bidirectionally. It uses Home Assistant as the
only abstraction layer: it does not access todo vendors or store vendor
credentials.

The runtime observes complete `TodoListEntity` snapshots and synchronizes additions,
status changes, renames, and safely mapped deletions in both directions. Persistent
UID mappings and conservative startup reconciliation avoid duplicates and data loss.

## Installation

1. Copy `custom_components/todo_sync` into the `custom_components` directory of
   your Home Assistant configuration.
2. Restart Home Assistant.
3. Open **Settings → Devices & services → Add integration** and select
   **Todo Sync**.
4. Select two different `todo` entities. Each entity must advertise support for
   creating, updating, and deleting todo items.

One config entry represents one unordered pair. Multiple independent pairs can
be configured, but the same pair cannot be configured twice.

The configuration stores entity-registry identifiers as durable references in
addition to the entity IDs selected by the user. This prepares the runtime to
follow future entity-ID changes without assuming that entity IDs are permanent.

## Development

Python 3.12 or newer is required. Create an isolated environment and install the
development dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
```

Run the checks:

```bash
ruff format --check .
ruff check .
pytest
python -m json.tool custom_components/todo_sync/manifest.json >/dev/null
```

The config-flow tests exercise successful setup, per-entity validation, all
required capability combinations, duplicate/reversed pairs, registry-reference
storage, and the basic setup/unload lifecycle.
