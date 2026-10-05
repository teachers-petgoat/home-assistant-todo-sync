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

Python 3.14.2 or newer is required. Tests intentionally pin
`pytest-homeassistant-custom-component` 0.13.367, which installs Home Assistant
2026.9.4. This is new enough to exercise the current `TodoListEntity` callback
contract and the Alexa Devices todo platform; advancing Home Assistant requires
updating the test fixture package pin in lockstep. Create an isolated environment
and install the development dependencies:

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

## Troubleshooting

Alexa Devices currently relies on `aioamazondevices` push/list consistency. An
upstream `itemCreated` push can arrive before the new UID is visible in the
following list read, so Home Assistant may briefly expose a snapshot that omits
the new item. Todo Sync treats single and rapid repeated omissions as removal
candidates rather than deletes. It only propagates a mapped deletion after a
later observation outside the stabilization interval; if the item returns, the
candidate is cancelled without changing the other provider.
