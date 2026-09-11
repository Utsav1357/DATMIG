# ADR-0004: Target-resident chunk ledger as the atomic checkpoint

- **Status:** Proposed (revised in Phase 0 v0.2)
- **Date:** 2026-09-10
- **Deciders:** DATMIG architecture
- **Supersedes:** —
- **Related:** ADR-0011 (deterministic plan-time chunk boundaries), ADR-0008 (loader strategy), ADR-0007 (control database), **ADR-0018 (source consistency model)**, **ADR-0019 (atomic reject records)**, **ADR-0020 (bootstrap vs runtime privileges)**

## Context

DATMIG must resume a multi-terabyte migration after an arbitrary crash without losing or duplicating rows. Data is written to Db2. Progress is naturally recorded in DATMIG's own control database (PostgreSQL).

That is two independent transactional resources. Without coordination there is a window between "the data committed to Db2" and "the progress record committed to PostgreSQL" in which a crash produces one of two bad outcomes:

- Progress written first → a crash before the data commits means the checkpoint claims rows that do not exist → **missing rows**, silently, forever.
- Data written first → a crash before the progress write means the chunk is replayed → **duplicate rows**, unless something else deduplicates.

Neither is acceptable for a platform that will be trusted with production data.

Additional constraints:

- Many chunks run concurrently across partitions, so a single monotonic high-water mark is insufficient — completion is not monotonic under parallelism.
- Resume must be cheap. Re-scanning the target at terabyte scale is not viable.
- The design must not couple Db2 availability to control-plane availability during normal running.

## Options considered

### 1. XA / two-phase commit across Db2 and PostgreSQL
Correct in theory. Requires a transaction manager, XA-capable drivers on both sides, distributed recovery handling and in-doubt transaction runbooks. Db2 supports XA, but XA through `ibm_db` from Python is an unusual, thinly documented path, and in-doubt transactions on a production Db2 are exactly what a DBA will refuse. **Rejected: disproportionate operational cost and risk.**

### 2. Progress in the control DB only, with post-hoc reconciliation
Reconciliation on a 2 TB table is a multi-hour full scan of both sides, and duplicate removal on a target without a unique constraint is genuinely hard. **Rejected: turns every crash into a major incident.**

### 3. Target-side deduplication via MERGE on every chunk
Correct, but substantially slower than insert, log-heavy, requires a reliable PK on every table, and is wasteful for the common case of loading into an empty target. **Rejected as the default; retained as `STAGED_MERGE` for incremental loads.**

### 4. Chunk-ID column on target rows
Pollutes the target schema permanently, changes row width, and asks the customer to accept a foreign column in production tables. **Rejected.**

### 5. Chunk ledger inside the target database, written in the same transaction as the data
A `DATMIG_CTL.CHUNK_LEDGER` table in the target Db2 database. Each chunk's `INSERT`s and its ledger row commit in one local transaction. The control database holds a mirror for reporting, refreshed after commit and rebuilt from the ledger on resume.

## Decision

**Adopt option 5**, extended in v0.2 to cover reject records.

```
BEGIN
  INSERT data rows                          (N array-insert batches)
  INSERT DATMIG_CTL.REJECT_RECORD rows      ← ADR-0019
  INSERT DATMIG_CTL.CHUNK_LEDGER row
COMMIT                                      ← the only durable event that matters
UPDATE control-DB mirrors                   ← after commit, best effort, retriable
```

Ledger primary key: `(MIGRATION_RUN_ID, TABLE_RUN_ID, PARTITION_ID, CHUNK_ID)`.

The checkpoint is never advanced before the corresponding Db2 transaction commits, because the checkpoint **is part of** that transaction. On resume, the ledger is authoritative and the control-DB mirror is reconciled from it.

`DATMIG_CTL` is created and maintained by a `DATMIG_BOOTSTRAP` role; the long-running runtime account holds only `INSERT`/`SELECT` on these two tables (ADR-0020). Requiring standing `CREATE TABLE` authority for a data-mover was an unnecessary privilege ask and has been removed.

## What this decision does and does not claim (v0.2)

**It delivers Target Atomic Chunk Commit unconditionally**: data rows, reject records and the ledger row commit together or not at all, under every source consistency mode, at any worker count.

**It does not deliver end-to-end exactly-once migration semantics on its own.** That stronger claim needs six invariants (Phase 0 architecture §12.6), of which this ADR supplies two (I5, and I6 with ADR-0019). The others come from ADR-0011 (immutable plan, deterministic boundaries), the transformation engine's purity rule (I4), and — critically — **ADR-0018**, because a stable source view is something the checkpoint design cannot manufacture. A migration run under `READ_COMMITTED_VERSIONED` still enjoys every property of this ADR and is still not exactly-once.

The v0.1 text of this ADR used "exactly-once" where it meant "atomic chunk commit". That wording was wrong and is corrected here and throughout the architecture.

## Consequences

### Positive
- No duplicate and no lost rows relative to what was read, with no distributed transaction.
- Resume cost is one indexed read of the ledger, not a table scan.
- Safe under arbitrary parallelism — each chunk is independent and self-describing.
- The ledger doubles as an audit artifact and carries the per-chunk digest used by `STRICT` validation, plus the digest algorithm and canonical encoding version so old digests cannot be silently misinterpreted.
- Reject accounting inherits the same guarantee (ADR-0019).
- A crash costs at most one chunk of re-work per active worker (≤100 MB by default).

### Negative
- DATMIG requires a `DATMIG_CTL` schema in the target database, created once by an elevated bootstrap role. This must be negotiated with the target DBA and is a real adoption cost. **If it is refused outright, this ADR is not viable and a materially weaker fallback is required** — flagged as an open question.
- The ledger and reject rows add to the target's transaction-log volume within the same transaction as the data. Ledger cost is negligible (one row per ~100 MB); reject cost is bounded by `max_reject_rows_per_chunk`.
- The control DB is explicitly *not* the source of truth for progress, which is counter-intuitive and must stay documented so nobody "fixes" it later.
- Loaders that cannot join the unit of work cannot write directly to the target. The Db2 LOAD utility is not DML, is not part of the caller's unit of work, and on failure leaves the table in **load pending** rather than rolling back; CLI LOAD is documented as non-atomic. All LOAD-based paths must therefore target disposable staging objects, with a regular DML `INSERT ... SELECT` plus ledger plus rejects as the commit step (architecture §7.7, ADR-0008).
- If the target is restored from a backup taken mid-migration, the ledger is restored with it — correct behaviour, and stated explicitly in the runbook.

### Neutral
- Adding a new target platform requires implementing `LedgerStore` and `RejectStore` for it: roughly 150 lines and two `CREATE TABLE`s.

## Verification

Verified by the recovery test in architecture §23.2: SIGKILL a migration between 40% and 70% completion, restart, resume, and assert zero missing rows, zero duplicate rows, exact row-count equality, per-chunk digest equality, reject-record counts matching the ledger's `rejected_rows` and the known seeded reject set with no duplicated `reject_seq`, and at most one re-executed chunk per active partition. Run with randomised kill timing on every change to the pipeline, checkpoint or Db2 connector packages.
