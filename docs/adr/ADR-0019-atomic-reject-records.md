# ADR-0019: Atomic reject records in the target transaction

- **Status:** Proposed
- **Date:** 2026-09-10
- **Deciders:** DATMIG architecture
- **Related:** ADR-0004 (target-resident chunk ledger), ADR-0011 (deterministic chunk boundaries), architecture §14.4 and §12.6

## Context

ADR-0004 removed the dual-write problem between target data and progress state by putting the chunk ledger inside the target transaction. The v0.1 pipeline pseudocode then reintroduced the same problem one level down:

```python
batch, rejects = ctx.transformer.apply(...)
if rejects:
    ctx.quarantine.write(chunk, rejects)   # separate transaction / sink
```

Reject records were written to a side sink — a JSONL file or the PostgreSQL control database — outside the chunk transaction. The consequences are the same class of bug ADR-0004 was written to eliminate:

- Crash after the quarantine write, before the commit → reject records exist for a chunk that was rolled back and will be replayed, so the same rows are quarantined twice, or quarantined and then successfully migrated.
- Crash after the commit, before the quarantine write → the ledger says rows were rejected and there is no record of which ones or why. For an audit trail, this is the worst possible failure: the operator is told data was skipped and cannot find out what.

Reject records are not debug output. They are the authoritative statement of which source rows did not reach the target and why, and they are frequently the artifact a data owner or auditor asks for. They deserve the same guarantee as the data.

## Required invariant

> If a chunk is committed, its authoritative reject records exist.
> If a chunk is rolled back, its authoritative reject records do not exist.

## Options considered

| Option | Verdict |
|---|---|
| **Reject records in `DATMIG_CTL.REJECT_RECORD` in the target Db2 database, written in the same transaction as the data rows and the ledger row** | **Adopted.** Satisfies the invariant by construction, using the mechanism already established and already trusted by ADR-0004 |
| Reject records to a local JSONL file or the control DB after commit | The original bug. Rejected |
| Reject records in PostgreSQL, coordinated with Db2 | Needs XA. Rejected for the same reasons as in ADR-0004 |
| Buffer in memory, write after commit with retry | Narrows the window, does not close it. A process kill between commit and write still loses the evidence, and the buffer is unbounded for a pathological chunk |
| Store only an aggregate reject count in the ledger | Satisfies atomicity but discards the per-row diagnosis, which is the entire operational value |
| Write rejects back to a quarantine table in the **source** database | Needs write access to a source we deliberately keep read-only. Rejected |

## Decision

Create `DATMIG_CTL.REJECT_RECORD` in the target database alongside `CHUNK_LEDGER`, created by the `DATMIG_BOOTSTRAP` role (ADR-0020), with primary key:

```
(MIGRATION_RUN_ID, TABLE_RUN_ID, PARTITION_ID, CHUNK_ID, REJECT_SEQ)
```

`REJECT_SEQ` is a deterministic ordinal assigned in extraction order within the chunk. Because chunk boundaries are deterministic (ADR-0011) and transformations are pure (invariant I4), a replay of the same chunk regenerates the same reject keys rather than appending duplicates. That is invariant I6 — idempotent reject handling — and it is what makes a retried chunk safe for the reject path as well as the data path.

The chunk transaction becomes:

```
BEGIN
  INSERT data rows
  INSERT reject records for this chunk
  INSERT ledger row (including rejected_rows and rejected_digest)
COMMIT
UPDATE control-DB mirrors   ← asynchronous, best effort, retriable
```

The PostgreSQL `REJECTED_RECORD` table is retained as an **asynchronous mirror**, rebuilt from the target on resume — exactly the relationship the control DB already has with the ledger.

## Consequences

### Positive
- The invariant holds by construction, with no new mechanism and no distributed transaction.
- Reject counts become independently verifiable: validation reconciles summed ledger `rejected_rows`, the actual `REJECT_RECORD` count, and `extracted_rows − loaded_rows`; disagreement raises `LEDGER_INCONSISTENCY` rather than passing unnoticed.
- Rejects survive a `SIGKILL` with the same guarantee as migrated rows, which is now asserted in the recovery test.
- Operators query reject detail with SQL against the target, which is where they already are during a migration.

### Negative
- Reject volume consumes target transaction-log space inside the data transaction. Bounded by `max_reject_rows_per_chunk` (default 10,000); exceeding it **fails the chunk** rather than committing a pathological transaction, on the reasoning that a chunk which is mostly rejects indicates a mapping or policy error that should stop the run.
- `REJECTED_VALUE` is capped at 4,000 characters and truncated with an explicit marker. Truncation of a diagnostic string is acceptable; truncation of migrated data never is, and the two must not be confused in the implementation.
- Values from columns marked sensitive in the connection profile are **omitted entirely**, not redacted in place, so that the reject table cannot become an accidental exfiltration path for data the source protects.
- One more table in the customer's target database, and one more object the bootstrap role must create.
- Transformations must genuinely be deterministic for `REJECT_SEQ` to be stable. This raises the purity rule from a design preference to a correctness requirement, enforced by the transform stage having no access to clocks, randomness or mutable external state.

### Neutral
- The staged-load path writes rejects in the same transaction as the staging-to-target `INSERT ... SELECT`, so the invariant is unchanged there.

## Verification

- The recovery test (architecture §23.2) seeds a known set of rows that a transformation rule will reject, kills the process mid-run, resumes, and asserts: `REJECT_RECORD` count equals summed ledger `rejected_rows` equals the seeded set, with no duplicated `REJECT_SEQ` and no orphan reject rows for uncommitted chunks.
- A fault-injection test forces a rollback after reject rows are written and asserts no reject rows remain.
- A unit test asserts `REJECT_SEQ` assignment is stable across two independent runs of the same chunk over the same input.
- A privacy test asserts that a column marked sensitive never appears in `REJECTED_VALUE`.
