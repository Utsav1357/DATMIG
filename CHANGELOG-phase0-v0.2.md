# Phase 0 architecture — CHANGELOG v0.1 → v0.2

Revision pass only. No redesign. Every V1 decision listed as preserved in the review request is unchanged: modular monolith, canonical vendor-neutral metadata, bounded-memory extraction, Arrow `RecordBatch`, target-resident chunk ledger, array insert as the default Db2 loader, policy-driven mappings, restricted read-only MCP, deterministic plans, PostgreSQL control-plane mirror.

Files changed: `DATMIG-Phase0-Architecture.md`, `type-mapping-matrix.md`, `ADR-0004-target-resident-chunk-ledger.md`, `adr/README.md`, `config/examples/migration.example.yaml`, `config/examples/transformations.example.yaml`.
Files added: `ADR-0018-source-consistency-model.md`, `ADR-0019-atomic-reject-records.md`, this changelog.

---

## 1. Source consistency and point-in-time correctness — **substantive, highest impact**

**Was:** assumption A8 stated the source could be read "with `READ COMMITTED SNAPSHOT` or during a quiet window", treated as sufficient.

**Now:** new **§6A** and **ADR-0018** introduce an explicit `SourceConsistencyMode` (`QUIESCED`, `DATABASE_SNAPSHOT`, `SNAPSHOT_TRANSACTION`, `READ_COMMITTED_VERSIONED`, `CDC_RECONCILED`) and a `SourceConsistencyHandle` established once per run and passed to every worker. Each mode is analysed across all seven requested dimensions.

Findings that changed the design, not just the wording:

- RCSI gives **statement-level** consistency only. Tens of thousands of chunk queries observe tens of thousands of points in time.
- **SQL Server cannot share a snapshot-isolation view across sessions** — there is no `pg_export_snapshot` equivalent. Therefore `SNAPSHOT_TRANSACTION` gives one point in time *per connection* and is not a parallel-safe fix. This is why the fallback is not what it first appears to be.
- A **database snapshot** is the only mechanism giving one database-wide point in time *and* unrestricted parallelism, because it is a separate read-only database every connection can open. Available in all editions from SQL Server 2016 SP1.
- Workers now read `handle.read_database`, never the configured source database name — a one-line indirection that makes snapshot reads a configuration choice rather than an extraction-code change.
- Only `QUIESCED`, `DATABASE_SNAPSHOT` and post-cutover `CDC_RECONCILED` permit a point-in-time claim.
- Resume refuses a coherent-PIT claim if the original snapshot is gone, rather than silently continuing against a newer one and producing a torn dataset.
- Snapshot lifecycle added: feasibility probe at plan time, establishment before dispatch, sparse-file headroom monitoring, teardown and orphan detection.

**Consequential edits:** A8 rewritten; §2.3 and §2.4 diagrams show the snapshot as the read source; §8.6 no-PK options now depend on the mode; new roadmap phase **P6a**; risk 5 rewritten and risk 5a (sparse-file exhaustion) added; open question 4 rewritten around achievable modes; `migration.example.yaml` gains a full `source_consistency` block.

## 2. Exactly-once terminology — **substantive**

**Was:** "exactly-once" used loosely, including in ADR-0004, where the mechanism actually delivers atomic chunk commit.

**Now:** new **§12.6** defines two distinct claims and the six invariants the stronger one requires — I1 stable source view, I2 immutable plan, I3 deterministic chunk boundaries, I4 deterministic transformations, I5 atomic target data + ledger + rejects, I6 idempotent reject handling — with a table of which mechanism delivers each and when it fails. A derived-claims table maps invariant sets to claims.

A `SemanticsLevel` (`EXACTLY_ONCE`, `NO_DUPLICATES_NO_LOSS`, `TARGET_ATOMIC_ONLY`) is computed at plan time, stored on the plan and every run, and printed by the CLI, API, validation report and audit trail. DATMIG will not print a claim it has not earned, and a CI grep enforces this in source from P1 onward.

**Consequential edits:** §2.2 invariant restated; §13.1 rewritten as "effectively-once target delivery"; §8.6 notes that `SEQUENTIAL_UNSAFE` caps the whole run's level; ADR-0004 gains an explicit "what this does and does not claim" section; risk 13 (over-claiming) added.

## 3. Type mapping corrections — **substantive, three factual errors fixed**

**3A. Db2 `CHAR` maximum.** v0.1 said 254 bytes. Current Db2 LUW documentation gives **255 OCTETS / 63 CODEUNITS32**; Db2 10.5 documents 254. Corrected, and marked **version-dependent** — the limit is now read from the connected server at plan time rather than hard-coded.

**3B. SQL Server `VARCHAR(n)` under UTF-8 collations.** v0.1 applied a ×4 expansion whenever the source collation was UTF-8. Wrong: `VARCHAR(n)` declares **n bytes** regardless of collation, so a UTF-8 `VARCHAR(100)` maps to `VARCHAR(100 OCTETS)` with **no** expansion. Non-UTF-8 source collations get a **3×** ceiling (any single byte maps to a BMP code point, at most 3 UTF-8 bytes — e.g. cp1252 `0x80` → `U+20AC`), not 4×.

**3C. `NVARCHAR(n)` semantics.** `n` counts **UTF-16 code units**, not characters; a supplementary character consumes two. So `VARCHAR(n CODEUNITS32)` does *not* preserve the source's length semantics — it safely contains every legal source value while admitting strings the source could not hold. Worst-case UTF-8 storage is **3n** octets, not 4n.

**New fidelity level `WIDENING`** added between `LOSSLESS` and `POTENTIALLY_LOSSY`, because several mappings were previously misclassified as lossless. Widening is accepted by default but always itemised in the fidelity report.

**3D. Canonical string metadata.** `length: int` and an ambiguous `length_unit: str` replaced by explicit `length_value`, `length_unit` (`OCTETS` / `UTF16_CODE_UNITS` / `UTF32_CODE_UNITS` / `CHARACTERS`), `encoding`, `unicode`, `source_collation`, plus derived `max_octets` / `max_code_points` and profiled `max_observed_*`. A worked example table shows four source shapes and their metadata.

**Also:** `CODEUNITS32` costs 4 octets per declared unit for row-width accounting, so the default is now the `OCTETS` form; the DDL generator sums row width against the page-size limit and fails the plan before emitting DDL; `TINYINT`→`SMALLINT` and `sysname` reclassified as widening.

## 4. Reject / quarantine atomicity — **substantive**

**Was:** `ctx.quarantine.write(chunk, rejects)` outside the chunk transaction — the same dual-write bug ADR-0004 exists to eliminate, one level down.

**Now:** **ADR-0019** and rewritten **§14.4**. `DATMIG_CTL.REJECT_RECORD` lives in the target database and is written **inside** the chunk transaction, keyed `(run, table_run, partition, chunk, reject_seq)` with `reject_seq` assigned deterministically in extraction order, so a replay regenerates the same keys rather than appending duplicates. Invariant: a committed chunk has its reject records; a rolled-back chunk has none. Four alternatives evaluated and rejected, including buffer-and-retry (narrows the window, does not close it).

Bounded by `max_reject_rows_per_chunk` (default 10,000) — exceeding it fails the chunk. `REJECTED_VALUE` capped at 4,000 characters; sensitive columns **omitted entirely** rather than masked in place. The PostgreSQL table becomes an asynchronous mirror. Validation reconciles three independent reject numbers and raises `LEDGER_INCONSISTENCY` on disagreement.

Determinism of transformations is promoted from a design preference to a **correctness requirement**, because `reject_seq` stability depends on it.

**Consequential edits:** §7.10 pseudocode; §12.2 DDL; §12.3 commit ordering; kill test asserts reject integrity; `transformations.example.yaml` `quarantine:` block replaced by `rejects:`.

## 5. Db2 LOAD semantics — **rewritten for precision; decision unchanged**

**Was:** LOAD dismissed as "non-atomic and non-rollbackable".

**Now:** §7.7 restructured into five parts using IBM terminology: regular DML semantics; LOAD utility semantics (not DML, not in the caller's unit of work, almost no logging); **load pending** state and `SQL0668N` reason code 3; `LOAD RESTART` / `LOAD TERMINATE` and the **not load restartable** state reachable via rollforward after a failed load or restore from an online backup taken during load; `COPY YES` / `COPY NO` (leaves the table space in **backup pending**) / `NONRECOVERABLE` (rollforward skips the transaction and marks the table invalid) and their defaults; `SET INTEGRITY` pending; CLI LOAD.

The question is reframed correctly: **can the mechanism participate in the atomic data + ledger + rejects unit of work?** A table answers it per mechanism. Array insert remains the V1 default. The staged path gains five concrete rules, including a dedicated staging table space so `COPY NO`/`NONRECOVERABLE` effects can never touch real target data, and a crash-recovery step that checks `SYSIBMADM.ADMINTABINFO.LOAD_STATUS` and issues `LOAD TERMINATE` before retrying — without which a crashed staged load leaves the staging table unusable and every retry fails.

## 6. Ledger privilege model — **substantive**

**Was:** the runtime service account implicitly needed `CREATE TABLE` in the target.

**Now:** §18.1 and ADR-0020 define four roles — `DATMIG_BOOTSTRAP`, `DATMIG_RUNTIME`, `DATMIG_SOURCE_READER`, `DATMIG_SNAPSHOT_ADMIN` — with minimum permissions enumerated per role. The runtime account holds **DML only**: `INSERT`/`SELECT` on the two `DATMIG_CTL` tables and the target tables, `USE OF TABLESPACE`, and `LOAD` authority only when a LOAD strategy is enabled. Staging tables are **pre-created at bootstrap from the deterministic plan**, which is what removes the `CREATETAB` requirement. `DATMIG_SNAPSHOT_ADMIN` is separated because `CREATE DATABASE` is elevated, with the option for the DBA to create the snapshot and supply its name instead.

Startup assertions added: fail fast on a missing privilege naming the object and the privilege; warn and refuse in production if the account holds `DBADM`/`SYSADM`/`DATAACCESS`/`sysadmin` unless `allow_excess_privileges: true`; record the resolved privilege set in the audit trail.

## 7. Identifier safety — **substantive correction**

**Was:** "allowlist by regex per platform" — which rejects legal object names and offers false assurance.

**Now:** §18.2 and ADR-0021 distinguish three provenance classes: trusted catalog identifiers (valid by construction, must be quoted, **must not** be pattern-filtered), user-supplied identifiers (structurally validated *and* cross-checked against discovered catalog metadata), and hostile input (same treatment — correct quoting plus catalog cross-check is the defence, not blacklisting).

Structural validation now rejects **only** empty strings, embedded NUL and over-length names. Punctuation, spaces, quotes, reserved words and non-ASCII all pass. Identifiers are not Unicode-normalised, because normalisation changes identity. Quoting implemented as SQL Server `[` `]` with `]` → `]]`, and Db2 `"` `"` with `"` → `""`, with a six-row worked example table. `Sales]; DROP TABLE X--` is documented as a **legal table name** that must round-trip, not as an attack to be rejected. Db2's upper-case folding of undelimited identifiers is called out as the trap in the other direction.

## 8. Digest design — **substantive**

**Was:** order-independent `sum(xxhash64(row)) mod 2^64`, described as sufficient.

**Now:** §16.2 states the collision characteristics honestly: random single-row corruption detected with probability 1 − 2⁻⁶⁴, but **no collision resistance** (xxhash64 collisions are constructible, so the 2⁻⁶⁴ figure assumes a randomness a systematic bug need not respect), an **additive structure that is homomorphic and therefore cancellable** by compensating errors such as two rows swapping values, a 64-bit birthday bound of 2³², and a note that XOR would be worse because it cannot detect a duplicated row.

Replaced by two tiers plus a screening digest:

- **Tier 1 (default):** ordered cryptographic streaming digest — BLAKE3, or SHA-256 where FIPS applies — over canonically encoded rows in ascending partition-key order, 256-bit. Available for every partitioning strategy except `SEQUENTIAL_UNSAFE`, since deterministic chunks already require a unique ordering. Detects insertion, deletion, modification, reordering and duplication.
- **Tier 2:** keyed additive multiset digest over 128 bits, with a per-run random key, for the order-independent case. Keying removes constructed and accidental cancellation; addition rather than XOR preserves duplicate detection.
- **xxhash64** retained only as an explicitly non-authoritative screening digest.

`DIGEST_ALGORITHM` and `CANONICAL_ENCODING_VERSION` are stored per chunk; mismatched pairs report `NOT_COMPARABLE` rather than pass or fail. Digest checks self-disable with a recorded reason under incoherent source modes. Cost is flagged as a P9 benchmark input, not a claim.

## 9. P1-T1 updates — **minimal, scope unchanged**

P1 remains a repository/foundation task with no database implementation. Four changes, each traceable to a revision above:

1. **`identifiers.py`** rewritten to §18.2: `quote_identifier` plus `validate_user_identifier` that rejects only empty/NUL/over-length. Tests now assert legal-but-unusual names are **accepted** — a test that rejects `Order Details` is a failing test.
2. **New deliverable 6a, `src/datmig/models/`** — frozen value objects only, no behaviour: `SourceConsistencyMode`, `SourceConsistencyHandle`, `SemanticsLevel`, `LengthUnit`, `DigestAlgorithm`, `ChunkSpec`, `LedgerEntry`, `RejectRecord` (with `reject_seq`), `LoadOutcome`. Defining these contracts now is what stops P2–P7 drifting.
3. **`settings.py`** gains `source_consistency`, reject limits, digest settings and `allow_excess_privileges`, with one validation rule: `READ_COMMITTED_VERSIONED` without `allow_incoherent_source: true` is a configuration error.
4. **`protocols.py`** — `ConnectionFactory.connect` takes an optional `database` override; `Loader` gains `write_reject_records` (documented as called inside the caller's transaction) and `recover_staging_if_needed`.

Plus a CI grep for unguarded "exactly-once" in source, and an explicit out-of-scope line: no digest implementation, no snapshot creation, and `models/` holds data shapes only.

## 10. Other consequential changes

- ADR list grows from 17 to 21; ADR-0008, 0010, 0012, 0017 retitled to match the revisions; 0018–0021 added; 0004, 0018, 0019 written in full.
- `adr/README.md` gains a reading order for new engineers.
- Roadmap: P6a inserted; P2 gains privilege assertions and the loader/CLI-LOAD spike; P7 gains reject records and the bootstrap DDL package; P9 gains the two-tier digest work.
- Risks: 5 rewritten, 5a, 13, 14, 15 added.
- Open questions: Q4 rewritten as the consistency-mode question; Q13 (will the DBA grant `DATMIG_BOOTSTRAP`) and Q14 (FIPS) added.
- POC and kill test: snapshot establishment, a concurrent writer proving reads come from the snapshot, seeded rejects, and an assertion that resume without the original snapshot refuses or downgrades.
