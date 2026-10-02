# AGENTS.md

This repository contains a Home Assistant custom integration for bidirectional synchronization between two `todo` entities.

The primary consumer of this file is Codex. Human readability is secondary to precise implementation guidance.

## Core goal

Implement a generic Home Assistant custom integration named `Todo Sync`.

The integration synchronizes two user-selected Home Assistant `todo` entities bidirectionally.

The integration MUST NOT communicate directly with external services such as Amazon Alexa, Bring!, AnyList, Grocy, or similar systems.

It MUST interact only with Home Assistant entities and Home Assistant APIs.

## Primary use case

Initial target:

- List A: Alexa Devices shopping list
- List B: Bring! shopping list

The implementation must remain generic and must not contain Alexa- or Bring-specific sync logic unless unavoidable due to Home Assistant entity behavior.

## Architecture principles

- Home Assistant is the abstraction layer.
- Treat both sides as `todo` entities.
- Do not use vendor APIs.
- Do not store vendor credentials.
- Do not depend on entity IDs remaining constant.
- Use config entry references and entity registry mechanisms where appropriate.
- Use modern async Home Assistant patterns.
- No blocking I/O.
- Full type annotations.
- Prefer small focused modules over large files.
- Avoid unnecessary dependencies.
- Follow current Home Assistant custom integration conventions.

## Supported synchronization for v1

Synchronize:

- item creation
- item deletion
- item summary/name changes
- status `needs_action`
- status `completed`
- reopening completed items

Do not synchronize in v1:

- descriptions
- due dates
- due datetimes
- ordering
- categories
- vendor-specific metadata

Descriptions and unsupported metadata on either side MUST be preserved when updating shared fields.

## Identity and mapping

Each source list has its own independent item UID.

Never assume the same UID exists on both sides.

Maintain persistent mappings between corresponding items.

Conceptually:

```text
pair_id
  list_a_uid
  list_b_uid
  normalized_summary
```

Mappings must survive Home Assistant restarts.

A mapping must be repairable through reconciliation if one or both stored UIDs become invalid.

## Summary normalization

Use normalized summaries only for matching/deduplication.

At minimum normalize by:

- trimming leading/trailing whitespace
- Unicode-safe case folding

Do not automatically alter the visible user-facing item name unless required by a synchronization operation.

Do not perform fuzzy matching in v1.

## Duplicate behavior

Do not create a second active equivalent item on the target list when an equivalent mapped or normalized item already exists.

Duplicates already present before setup must not be silently deleted.

Ambiguous duplicate matches must be handled conservatively and logged.

## Event handling

Do not rely exclusively on Home Assistant automation triggers such as `todo.item_added`.

The integration should observe complete todo item snapshots/state updates and calculate deltas.

The sync engine must recognize:

- add
- delete
- rename
- completed
- reopened

Maintain previous snapshots for comparison.

## Loop prevention

Every operation performed by the integration may trigger another update event.

The integration must prevent ping-pong synchronization.

Use one or more of:

- expected/pending operation tracking
- UID mapping
- state comparison
- idempotent target operations

Never rely only on arbitrary sleep delays for loop prevention.

All operations should be idempotent wherever possible.

## Reconciliation

Run reconciliation:

- after integration startup
- after configuration reload
- after either tracked todo entity becomes available again
- optionally through an explicit internal method callable by tests

Reconciliation must:

- read both complete lists
- validate stored mappings
- recreate missing mappings where unambiguous
- repair missing corresponding items where safe
- synchronize statuses where safe
- avoid destructive action in ambiguous situations

Reconciliation must prioritize data preservation.

If uncertain, log the conflict instead of deleting data.

## Conflict handling

Normal realtime synchronization:

- the newly observed user-originated change should be propagated to the other side

Startup/reconciliation:

- never guess destructively
- active (`needs_action`) should generally be preferred over completed when matching equivalent items with conflicting status, because this avoids accidentally hiding an intended purchase
- ambiguous rename/delete situations must be logged instead of guessed

Conflict rules must be centralized and tested.

## Delete safety

Delete operations are high risk.

A delete must only propagate when the integration can reliably determine the corresponding mapped target item.

Never delete a target item based solely on fuzzy or uncertain matching.

If a delete cannot be mapped reliably:

- do not delete anything on the opposite list
- remove or repair stale mappings as appropriate
- log a warning

## Rename handling

A todo provider may implement rename by replacing an item and changing its UID.

Therefore a rename must not assume stable UIDs.

The integration should detect semantic replacement where possible using:

- previous snapshot
- new snapshot
- persistent mapping
- operation context

Mappings must be updated after UID changes.

## Entity availability

If either todo entity is unavailable:

- do not perform destructive synchronization
- keep internal state consistent
- retry through later reconciliation when the entity returns

No data should be removed merely because an entity is temporarily unavailable.

## Config flow

Configuration must be UI-based.

User selects:

- Todo entity A
- Todo entity B

Validation:

- both must be `todo` entities
- they must be different entities
- required create/update/delete capabilities should be detected
- clearly reject unsupported combinations where core v1 behavior cannot work

One config entry represents one synchronized pair.

Support multiple config entries for multiple independent pairs.

## Storage

Use Home Assistant's supported persistent storage mechanisms.

Do not create arbitrary files in the Home Assistant config directory.

Stored data should include only what is necessary:

- schema/version
- pair mappings
- minimal reconciliation metadata

Storage format must be versioned and migratable.

## Logging

Use structured and useful debug logging.

Do not spam normal logs.

Important events:

- sync propagation
- mapping creation/removal
- reconciliation results
- conflicts
- unavailable entities
- rejected destructive operations

Never log credentials or unrelated user data.

## Tests

Tests are mandatory.

Minimum test coverage:

- config flow success
- same entity selected twice
- unsupported entity rejection
- add A -> B
- add B -> A
- duplicate prevention
- complete A -> B
- complete B -> A
- reopen A -> B
- reopen B -> A
- rename A -> B
- rename B -> A
- rename where provider changes UID
- delete A -> B
- delete B -> A
- delete without reliable mapping does not delete target
- loop prevention
- restart with valid mappings
- restart with stale mappings
- startup reconciliation
- target temporarily unavailable
- recovery after target becomes available
- conflicting status on reconciliation
- pre-existing duplicate ambiguity

A bug affecting delete, rename, mapping, or reconciliation must receive a regression test.

## Development workflow

Do not make unrelated refactors.

Prefer small PR-sized changes.

Before considering a task complete:

- run formatter/linter
- run tests
- run Home Assistant validation tools where configured
- update tests for behavioral changes

Do not weaken tests to make implementation pass.

## v1 non-goals

Do not add unless explicitly requested:

- HACS publication metadata
- diagnostics UI
- sensors
- buttons
- services
- dashboards
- vendor-specific handling
- fuzzy matching
- advanced conflict UI
- manual pair editing UI
- syncing more than two lists per config entry
