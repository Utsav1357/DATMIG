# ADR-0018: Source consistency model and point-in-time correctness

- **Status:** Proposed
- **Date:** 2026-09-10
- **Deciders:** DATMIG architecture
- **Related:** ADR-0004 (target-resident chunk ledger), ADR-0011 (deterministic chunk boundaries), architecture §6A and §12.6

## Context

Phase 0 v0.1 assumed the source could be read "with `READ COMMITTED SNAPSHOT` or during a quiet window" and treated that as sufficient for a correct migration. It is not, and the imprecision propagated into an over-strong exactly-once claim.

**FACT.** RCSI provides *statement-level* read consistency: each statement sees committed state as of the moment it began. A multi-terabyte migration issues tens of thousands of chunk queries over many hours; under RCSI each observes a different point in time. RCSI eliminates reader/writer blocking. It does not create a shared snapshot.

**FACT.** SQL Server provides no mechanism to export a snapshot-isolation view from one session and adopt it in another — there is no equivalent of PostgreSQL's `pg_export_snapshot`. N parallel workers under `SNAPSHOT` isolation therefore hold N independent snapshots taken at N different moments. **Parallel workers and a single transaction-scoped snapshot are mutually exclusive on SQL Server.** This is the decisive finding: the obvious fix for RCSI does not work.

**FACT.** A SQL Server **database snapshot** is a read-only, static view of the source, transactionally consistent as of creation, minus uncommitted transactions. It is a separate database on the same instance, so every worker connection reads the same point in time concurrently. Available in all editions from SQL Server 2016 SP1 (Enterprise-only before). Backed by NTFS sparse files maintained by copy-on-write of source pages.

Point-in-time coherence is therefore a property of the **source read strategy**, and no amount of target-side atomicity can substitute for it.

## Decision

Introduce an explicit, first-class `SourceConsistencyMode` with five values — `QUIESCED`, `DATABASE_SNAPSHOT`, `SNAPSHOT_TRANSACTION`, `READ_COMMITTED_VERSIONED`, `CDC_RECONCILED` — and a `SourceConsistencyHandle` established once per run, persisted on the run record, and passed to every worker. Workers read from `handle.read_database`, never from the configured source database name.

**Default: `DATABASE_SNAPSHOT`** where the edition, the disk headroom and the snapshot-creation privilege allow; otherwise `QUIESCED`. `READ_COMMITTED_VERSIONED` is permitted only with `allow_incoherent_source: true`. `CDC_RECONCILED` is reserved in the model and out of scope for V1.

Rationale for the default: it is the only mode that provides a single database-wide point in time *and* unrestricted parallelism, and it does not require application downtime. `SNAPSHOT_TRANSACTION` is explicitly **not** the fallback, because it delivers neither: one point in time per connection, plus a `tempdb` version store that must retain every version created since the oldest reader began — the failure mode most likely to cause a production incident on a busy source.

Full per-mode analysis — consistency guarantee, operational requirements, source impact, parallel-worker compatibility, `tempdb`/version-store implications, retry semantics and validation implications — is in architecture §6A.3.

### Modes that permit the point-in-time claim

`QUIESCED`, `DATABASE_SNAPSHOT`, and `CDC_RECONCILED` after cutover. Nothing else.

### Enforcement

1. **Plan time** — feasibility probed (edition, RCSI state, `ALLOW_SNAPSHOT_ISOLATION`, permissions, disk headroom). An infeasible mode is a blocking issue, not a warning.
2. **Run start** — the handle is established before any worker is dispatched; failure means the run does not start.
3. **Per chunk** — no code path reads the live source while a snapshot is in force.
4. **Resume** — if the original snapshot is gone, the point in time is gone. DATMIG refuses to resume with a coherent-PIT claim and requires either a new snapshot plus a full re-run of incomplete tables, or explicit acceptance of degradation. Silently resuming against a newer snapshot would produce a torn dataset and is the single worst outcome available here.
5. **Run end** — the snapshot is dropped as a `post_run_action`, with retry and alerting; an orphaned database snapshot grows without bound and is an operational hazard.

### Semantics reporting

The orchestrator computes a `SemanticsLevel` (`EXACTLY_ONCE`, `NO_DUPLICATES_NO_LOSS`, `TARGET_ATOMIC_ONLY`) from the consistency mode together with the other invariants of §12.6, records it on the plan and on every run, and prints it in the CLI, the API, the validation report and the audit trail.

## Consequences

### Positive
- The correctness claim is now derivable from configuration rather than assumed.
- The `read_database` indirection makes `DATABASE_SNAPSHOT` a configuration choice rather than an extraction-code change.
- Validation knows when count equality is meaningful and when it is not, eliminating a large class of false failures.
- The failure mode of a live source is now explicit and visible instead of surfacing months later as unexplained row-count drift.

### Negative
- `DATABASE_SNAPSHOT` imposes copy-on-write I/O on the source for the whole migration, and sparse-file growth tracks **write churn**, not database size. This must be sized with the DBA and monitored during the run; exhaustion makes the snapshot suspect and fails the run.
- It requires `CREATE DATABASE` permission to establish, which is elevated. Mitigated by separating `DATMIG_SNAPSHOT_ADMIN` from the runtime account, or by having the DBA create the snapshot and supplying its name as configuration (ADR-0020).
- While a snapshot exists, the source database cannot be dropped, detached or restored — a real constraint on the DBA's options during a long migration.
- Adds a run-lifecycle step that can fail, plus an orphan-cleanup responsibility.
- Some customers will simply not be able to offer any coherent mode. The architecture accommodates that, at the price of a reduced and clearly stated `SemanticsLevel`.

### Neutral
- `CDC_RECONCILED` is designed for but not implemented. The enum value and the `watermark` field exist so that adding it later does not reshape the run record.

## Verification

- An integration test runs a migration with a concurrent writer active against the live source and asserts that rows written after snapshot creation are **absent** from the target under `DATABASE_SNAPSHOT`, and **present in a non-reproducible pattern** under `READ_COMMITTED_VERSIONED` — proving the modes actually differ rather than merely being labelled differently.
- A resume test drops the snapshot between runs and asserts DATMIG refuses the coherent-PIT claim rather than continuing silently.
- An orphan test kills the process after snapshot creation and asserts the cleanup path detects and removes it.
- A unit test asserts `SemanticsLevel` can only be downgraded by later evidence, never upgraded.
