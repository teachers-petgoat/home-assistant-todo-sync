# Todo Sync — Synchronization Specification

## 1. Purpose

`Todo Sync` is a Home Assistant custom integration that keeps two selected Home Assistant `todo` entities synchronized bidirectionally.

Example:

```text
Alexa shopping list ⇄ Todo Sync ⇄ Bring! shopping list
```

The integration operates entirely inside Home Assistant.

It does not connect directly to Alexa, Bring!, or any other provider.

## 2. Scope

Version 1 synchronizes the common subset required for shopping lists:

```text
summary
status
```

Supported operations:

```text
ADD
DELETE
RENAME
COMPLETE
REOPEN
```

Unsupported metadata is left untouched.

## 3. Terminology

### List A / List B

The two configured Home Assistant `todo` entities.

There is no permanent master list.

Both sides may receive user changes.

### UID

Provider-specific identifier of one todo item.

Example:

```text
Alexa UID: amazon-123
Bring UID: bring-a94f
```

UIDs are local to their respective list.

### Pair

Internal association between equivalent items.

Example:

```text
amazon-123 ↔ bring-a94f
```

### Normalized summary

A normalized representation used for matching.

Example:

```text
" Milch " → "milch"
"MILCH"   → "milch"
```

## 4. Item model

Internal representation:

```text
TodoSyncItem
- uid
- summary
- normalized_summary
- status
```

Status values relevant to v1:

```text
needs_action
completed
```

Provider-specific metadata may be retained in snapshots when required but must not become part of v1 synchronization semantics.

## 5. Persistent mapping

Store an internal mapping per synchronized pair.

Example:

```json
{
  "version": 1,
  "pairs": {
    "pair-uuid": {
      "a_uid": "amazon-123",
      "b_uid": "bring-a94f",
      "normalized_summary": "milch"
    }
  }
}
```

The internal pair ID is stable even if one provider replaces its UID after a rename.

## 6. Add operation

Initial:

```text
A:
Milk

B:
-
```

Detection:

```text
new item in A
```

Procedure:

1. Normalize source summary.
2. Look for existing mapping.
3. Search target for exact normalized equivalent.
4. If an unambiguous equivalent exists:
   - create/update mapping
   - synchronize status if required
   - do not create duplicate.
5. Otherwise create target item.
6. Read resulting target state.
7. Store UID mapping.

Expected result:

```text
A:
Milk

B:
Milk
```

## 7. Duplicate protection

Given:

```text
A:
Milk

B:
Milk
```

An update event must not create:

```text
B:
Milk
Milk
```

Exact normalized equality is used in v1.

No fuzzy matching.

Pre-existing duplicate cases:

```text
B:
Milk
Milk
```

must be treated as ambiguous unless an existing UID mapping resolves the intended item.

Do not delete duplicates automatically.

## 8. Complete operation

Initial:

```text
A:
Milk - needs_action

B:
Milk - needs_action
```

User completes Milk in A.

Result:

```text
A:
Milk - completed

B:
Milk - completed
```

The target update must preserve target-only metadata.

## 9. Reopen operation

Initial:

```text
A:
Milk - completed

B:
Milk - completed
```

User reopens Milk in A.

Result:

```text
A:
Milk - needs_action

B:
Milk - needs_action
```

Reopening is a first-class operation and must not be treated as creating a new item.

## 10. Rename operation

Initial:

```text
A:
Milk [a1]

B:
Milk [b1]
```

User renames A:

```text
A:
Whole milk [a1]
```

Target becomes:

```text
B:
Whole milk
```

If B preserves its UID:

```text
a1 ↔ b1
```

remains valid.

If B replaces its UID:

```text
b1 → b2
```

mapping becomes:

```text
a1 ↔ b2
```

The integration must account for providers that implement rename as remove + recreate.

## 11. Delete operation

Initial:

```text
A:
Milk [a1]

B:
Milk [b1]

mapping:
a1 ↔ b1
```

User deletes A item.

The integration identifies `b1` through the stored pair and deletes exactly `b1`.

Result:

```text
A:
-

B:
-
```

Mapping is removed after successful propagation.

### Delete safety rule

If:

```text
a1
```

is deleted but no reliable mapping to a B UID exists, the integration MUST NOT infer a destructive target from an uncertain match.

Instead:

```text
log conflict
schedule/allow reconciliation
leave B unchanged
```

Data preservation has priority over strict convergence.

## 12. Loop prevention

Example:

```text
A adds Milk
↓
sync creates Milk in B
↓
B emits update
```

The B update must not create a second Milk in A.

Loop prevention mechanisms:

1. persistent pair mapping
2. expected operation tracking
3. snapshot comparison
4. idempotent existence checks

An event whose resulting state is already reflected on the opposite list requires no operation.

No fixed delay-based loop prevention.

## 13. Snapshots

The sync engine maintains the last known snapshot for each list.

Example:

```text
previous A:
a1 Milk needs_action

current A:
a1 Milk completed
```

Derived delta:

```text
COMPLETE a1
```

Another example:

```text
previous:
a1 Milk needs_action

current:
a1 Whole milk needs_action
```

Derived delta:

```text
RENAME a1 "Milk" → "Whole milk"
```

This supports changes not exposed as dedicated Home Assistant todo triggers.

## 14. Reconciliation

Reconciliation compares complete current snapshots of A and B against persistent mappings.

Run at least:

```text
integration startup
integration reload
entity recovery after unavailable
```

### Goals

- validate mappings
- remove clearly stale mappings
- rebuild mappings where exact matching is unambiguous
- create missing counterparts where safe
- align statuses where safe

### Safety

Reconciliation is conservative.

It must not propagate deletes purely because an item is missing on one side unless there is sufficient evidence that a delete occurred and the target mapping is reliable.

Startup must not turn an historical inconsistency into a destructive delete.

## 15. Initial setup behavior

Example existing state:

```text
A:
Milk
Bread

B:
Milk
Butter
```

On initial setup:

Exact match:

```text
Milk ↔ Milk
```

is mapped.

Unmatched active items are merged conservatively:

```text
Bread → B
Butter → A
```

Result:

```text
A:
Milk
Bread
Butter

B:
Milk
Bread
Butter
```

This establishes a common baseline.

Completed items require conservative handling.

An active item should generally win over an equivalent completed item:

```text
A: Milk needs_action
B: Milk completed
```

becomes:

```text
A: Milk needs_action
B: Milk needs_action
```

Reason: preserving an intended purchase is safer than accidentally marking it purchased.

## 16. Simultaneous conflicts

Example:

```text
A changes Milk → Whole milk

B changes Milk → Low-fat milk
```

before either change propagates.

The integration must not silently choose based on arbitrary ordering if both changes are independently observed.

For v1:

```text
detect conflict
log warning
avoid destructive overwrite where possible
```

A later version may add configurable conflict resolution.

## 17. Temporary unavailability

If A or B becomes unavailable:

```text
do not delete
do not infer empty list
do not reset mappings
```

When available again:

```text
refresh snapshots
reconcile
resume realtime synchronization
```

## 18. Provider metadata preservation

Example Bring item:

```text
summary: Milk
description: 2 litres
status: needs_action
```

Alexa changes Milk to completed.

The integration should update only:

```text
status
```

Target result:

```text
summary: Milk
description: 2 litres
status: completed
```

The description must remain intact.

## 19. Configuration

One config entry:

```text
Todo Sync

List A:
todo.alexa_shopping

List B:
todo.bring_family
```

Rules:

- A and B must both be `todo` entities.
- A and B must differ.
- Both must support the minimum required operations.
- Multiple independent config entries are allowed.

## 20. Minimum capabilities for v1

For full synchronization each entity should support:

```text
CREATE_TODO_ITEM
UPDATE_TODO_ITEM
DELETE_TODO_ITEM
```

If an entity lacks required capabilities, setup should fail with a clear explanation rather than silently running a partial sync.

## 21. Logging examples

Debug:

```text
Mapped A:a1 ↔ B:b7 for normalized summary "milk"
Propagating COMPLETE A:a1 -> B:b7
Reconciliation completed: 12 mappings, 0 conflicts
```

Warning:

```text
Delete not propagated: no reliable target mapping for A:a1
Ambiguous duplicate items for normalized summary "milk"
```

## 22. Non-goals for v1

Not included:

```text
fuzzy product matching
quantity semantics
categories
shopping-store grouping
ordering
due dates
description synchronization
more than two lists per config entry
vendor APIs
cloud authentication
HACS publishing
dashboard cards
custom services
```

## 23. Acceptance criteria

V1 is considered functional when two compatible HA `todo` entities can be configured and:

```text
Add       A ⇄ B
Complete  A ⇄ B
Reopen    A ⇄ B
Rename    A ⇄ B
Delete    A ⇄ B
```

work reliably without:

```text
duplicates
ping-pong loops
loss of target-only metadata
unsafe deletion
loss of mappings after restart
```

and startup reconciliation returns both lists to a consistent state where doing so is unambiguous and safe.
