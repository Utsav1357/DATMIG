# DATMIG Type Mapping Matrix — SQL Server → Canonical → Db2 LUW

**Status:** Phase 0 design reference
**Version:** 0.2 (corrected character-type semantics; see §0)
**Assumed target:** Db2 LUW 11.5.x, Unicode (UTF-8) database — see `VERIFY` notes

Fidelity levels:

- **LOSSLESS** — every source value survives exactly, and the target admits no value the source could not hold.
- **WIDENING** — every source value survives exactly, but the target admits values the source could not hold. Safe for migration; **not** a semantic equivalence, and must not be described as one. New in v0.2, because v0.1 misclassified several character mappings as lossless.
- **POTENTIALLY LOSSY** — safe for most data, but a specific input class can fail or lose information; DATMIG emits a plan warning that must be acknowledged.
- **REQUIRES POLICY** — no single correct answer; the plan fails unless configuration names a policy.
- **UNSUPPORTED** — no mapping; the column must be excluded or given a custom transform.

## 0. v0.2 corrections

| # | v0.1 said | Correct position |
|---|---|---|
| C1 | Db2 `CHAR` maximum is 254 bytes | **255 OCTETS or 63 CODEUNITS32** on current Db2 LUW. Db2 10.5 documentation states 254, so the limit is **version-dependent** and DATMIG reads it from the connected server rather than hard-coding it |
| C2 | A UTF-8 source collation means multiply `VARCHAR(n)` by four | Wrong. **SQL Server `VARCHAR(n)` declares n bytes, including under `_UTF8` collations.** A UTF-8 `VARCHAR(100)` holds 100 bytes and maps to `VARCHAR(100 OCTETS)` with **no** expansion |
| C3 | `CODEUNITS32` preserves `NVARCHAR` length semantics | Wrong. `NVARCHAR(n)` counts **UTF-16 code units**, not characters; a supplementary character consumes two of them. `VARCHAR(n CODEUNITS32)` safely contains every legal source value but admits strings the source could not hold — that is a **widening** |
| C4 | String length is a single integer | Replaced by explicit canonical metadata: `length_value`, `length_unit`, `encoding`, `unicode`, `source_collation` (§2) |

---

## 1. Hard limits used throughout

Verified against IBM documentation, but **read from the live catalog at plan time** rather than trusted from this table:

- `VARCHAR` maximum: **32672 OCTETS** or **8168 CODEUNITS32**.
- `CHAR` maximum: **255 OCTETS** or **63 CODEUNITS32** (Db2 10.5: 254 — **VERIFY** for your version).
- `DECIMAL` maximum precision: **31**.
- `TIMESTAMP(p)`: `p` between **0 and 12**.
- `CLOB` / `BLOB`: up to **2 GB** (`CLOB` up to 536,870,911 CODEUNITS32).
- Maximum row length is bound by table space page size (approximately 4005 / 8101 / 16293 / 32677 bytes for 4K / 8K / 16K / 32K pages). LOB data counts as a descriptor, not inline.
- `CODEUNITS32` reserves **4 octets per declared code unit** for row-width accounting. `VARCHAR(8168 CODEUNITS32)` equals `VARCHAR(32672 OCTETS)`. This matters for wide tables.

SQL Server side:

- `VARCHAR(n)` / `CHAR(n)`: `n` is **bytes**, 1–8000. `MAX` means LOB.
- `NVARCHAR(n)` / `NCHAR(n)`: `n` is **UTF-16 code units** (2-byte units), 1–4000. Storage is `2n` bytes. `MAX` means LOB.
- A `_UTF8` collation changes the encoding of `VARCHAR`/`CHAR` data to UTF-8. It does **not** change `n` from bytes to characters.

---

## 2. Canonical string metadata

A single integer cannot express any of the above, which is how the v0.1 errors happened. The canonical model carries:

| Field | Meaning |
|---|---|
| `length_value` | The declared length, as declared |
| `length_unit` | `OCTETS` \| `UTF16_CODE_UNITS` \| `UTF32_CODE_UNITS` \| `CHARACTERS` |
| `encoding` | Storage encoding of the source column: `utf-16le`, `utf-8`, `cp1252`, `cp932`, … |
| `unicode` | Whether the column can represent the full Unicode repertoire |
| `source_collation` | Full collation name, retained for diagnostics and for comparison-semantics warnings |
| `max_octets` | Derived worst-case storage in the source encoding |
| `max_code_points` | Derived worst-case count of distinct Unicode code points |
| `max_observed_octets` / `max_observed_code_points` | From discovery-time profiling; drives LOB downgrade decisions |

Worked examples:

| Source column | `length_value` | `length_unit` | `encoding` | `unicode` | `max_octets` | `max_code_points` |
|---|---|---|---|---|---|---|
| `VARCHAR(100)`, `Latin1_General_CI_AS` | 100 | OCTETS | cp1252 | no | 100 | 100 |
| `VARCHAR(100)`, `..._SC_UTF8` | 100 | OCTETS | utf-8 | yes | 100 | 100 (1-byte chars) … 25 (4-byte chars) |
| `NVARCHAR(100)` | 100 | UTF16_CODE_UNITS | utf-16le | yes | 200 | 100 (BMP) … 50 (all supplementary) |
| `CHAR(10)`, DBCS collation | 10 | OCTETS | cp932 | no | 10 | 10 … 5 |

### Target sizing rules derived from this metadata

Let the target be a UTF-8 Db2 database.

| Source shape | Safe target in OCTETS | Safe target in CODEUNITS32 | Classification |
|---|---|---|---|
| `VARCHAR(n)`, `_UTF8` collation | `n` — **no expansion** | `n` (over-provisions 4n octets of row width) | LOSSLESS in OCTETS form |
| `VARCHAR(n)`, SBCS collation | `3n` worst case — any single byte maps to one BMP code point, which is at most 3 UTF-8 bytes (e.g. cp1252 `0x80` → `U+20AC` → 3 bytes) | `n` | WIDENING |
| `VARCHAR(n)`, DBCS/MBCS collation | `3n` bound (tighter in practice; a 2-byte DBCS character is at most 3 UTF-8 bytes) | `n` | WIDENING |
| `NVARCHAR(n)` | `3n` — BMP code units cost at most 3 bytes; a surrogate pair is 2 code units and 4 UTF-8 bytes, i.e. 2 bytes per code unit | `n` — the source can hold at most `n` code points | **WIDENING** in both forms |

**RECOMMENDATION.** Default to the `OCTETS` form sized by the rules above, because it is the tighter fit against the page-size row limit and, for UTF-8 source collations, it is genuinely lossless. Use `CODEUNITS32` when the target application expects character-count semantics and the row-width budget allows — policy key `string_units`. Either way the fidelity report classifies the result honestly, and the theoretical bound is only a ceiling: DATMIG profiles `MAX(DATALENGTH(col))` during discovery and will size from measured data when `size_from_profile: true`, which frequently halves the declared widths on legacy schemas.

**Row-width enforcement.** The DDL generator sums the declared target widths for each table and compares them against the table space page-size limit **before** emitting DDL, failing the plan with the offending columns named. Discovering this from a Db2 `SQL0670N` at DDL time is a worse experience and happens after the plan was approved.

---

## 3. Exact numeric

| SQL Server | Canonical | Db2 target | Fidelity | Notes |
|---|---|---|---|---|
| `BIT` | `BOOLEAN` | `SMALLINT` (0/1) **default** | LOSSLESS | `BOOLEAN` as a column type is Db2-version dependent — **VERIFY**. `SMALLINT` is portable and indexable everywhere. NULL stays NULL. Policy `bit_strategy: smallint \| boolean \| char1` |
| `TINYINT` | `INT16` | `SMALLINT` | WIDENING | SQL Server `TINYINT` is unsigned 0–255; Db2 has no `TINYINT`. `SMALLINT` covers it and also admits negatives the source could not hold |
| `SMALLINT` | `INT16` | `SMALLINT` | LOSSLESS | Identical range |
| `INT` | `INT32` | `INTEGER` | LOSSLESS | |
| `BIGINT` | `INT64` | `BIGINT` | LOSSLESS | |
| `DECIMAL(p,s)` / `NUMERIC(p,s)`, p ≤ 31 | `DECIMAL(p,s)` | `DECIMAL(p,s)` | LOSSLESS | |
| `DECIMAL(p,s)`, 31 < p ≤ 38 | `DECIMAL(p,s)` | policy | **REQUIRES POLICY** | Db2 caps `DECIMAL` at precision 31. Options: `narrow_if_safe` (**recommended**) maps to `DECIMAL(31,s)` after a verified `MAX(ABS(col))` profile; `decfloat` → `DECFLOAT(34)`, which changes exactness and comparison semantics; `varchar` → lossless but loses arithmetic; `fail` |
| `MONEY` | `DECIMAL(19,4)` | `DECIMAL(19,4)` | LOSSLESS | Range ±922,337,203,685,477.5807 |
| `SMALLMONEY` | `DECIMAL(10,4)` | `DECIMAL(10,4)` | LOSSLESS | Range ±214,748.3647 |

## 4. Approximate numeric

| SQL Server | Canonical | Db2 target | Fidelity | Notes |
|---|---|---|---|---|
| `REAL` / `FLOAT(1..24)` | `FLOAT32` | `REAL` | LOSSLESS | Both IEEE 754 single |
| `FLOAT` / `FLOAT(25..53)` | `FLOAT64` | `DOUBLE` | LOSSLESS | Both IEEE 754 double |

Validation note: `SUM` over float columns is order-dependent. Validation compares with a relative tolerance, or skips with a recorded reason.

## 5. Character

Sizing follows §2. `m` below denotes the computed safe target length.

| SQL Server | Canonical | Db2 target | Fidelity | Notes |
|---|---|---|---|---|
| `CHAR(n)`, `m` ≤ 255 OCTETS (or n ≤ 63 CU32) | `FIXED_STRING`, OCTETS | `CHAR(m)` | LOSSLESS or WIDENING per §2 | Both platforms blank-pad. Assumes `ANSI_PADDING ON` on the source |
| `CHAR(n)`, `m` > the `CHAR` ceiling | `FIXED_STRING` | `VARCHAR(m)` + pad policy | POTENTIALLY LOSSY | Blank-padding semantics change. Policy `char_over_limit: varchar_padded \| varchar_trimmed \| fail` |
| `NCHAR(n)`, n ≤ 63 | `FIXED_STRING` unicode, UTF16_CODE_UNITS | `CHAR(n CODEUNITS32)` | WIDENING | Above 63 CU32 it must become `VARCHAR` |
| `VARCHAR(n)`, `_UTF8` collation | `STRING`, OCTETS, `encoding = utf-8` | `VARCHAR(n OCTETS)` | **LOSSLESS** | The v0.1 ×4 expansion here was an error |
| `VARCHAR(n)`, non-UTF8 collation | `STRING`, OCTETS, `encoding = <code page>` | `VARCHAR(m OCTETS)`, `m ≤ 3n` | WIDENING | Profile-based sizing usually gives `m` far below `3n` |
| `NVARCHAR(n)`, `m` within the `VARCHAR` ceiling | `STRING` unicode, UTF16_CODE_UNITS | `VARCHAR(3n OCTETS)` or `VARCHAR(n CODEUNITS32)` | **WIDENING** | `NVARCHAR(4000)` → `VARCHAR(12000 OCTETS)` or `VARCHAR(4000 CODEUNITS32)`. Both exceed a 4K/8K/16K page row budget on their own, so page size must be checked |
| `NVARCHAR(n)` where `m` exceeds the `VARCHAR` ceiling | `TEXT` | `CLOB` | POTENTIALLY LOSSY | Loses indexability and predicate performance. Reachable only for very large `n` combined with a high expansion factor |
| `VARCHAR(MAX)` / `NVARCHAR(MAX)` | `TEXT` | `CLOB(2G)` **or** downgraded `VARCHAR` | **REQUIRES POLICY** | Policy `lob_strategy: clob \| downgrade_if_safe \| fail`. `downgrade_if_safe` needs a discovery-time `MAX(DATALENGTH(col))` probe and a safety margin. Frequently a 5–20× load throughput improvement |
| `TEXT` | `TEXT` | as `VARCHAR(MAX)` | **REQUIRES POLICY** | Deprecated in SQL Server |
| `NTEXT` | `TEXT` unicode | as `NVARCHAR(MAX)` | **REQUIRES POLICY** | Deprecated in SQL Server |

Collation note: SQL Server collation controls comparison, padding and case sensitivity; Db2 collation is database-level. Sort-order differences do **not** affect data fidelity but **do** affect `MIN`/`MAX` validation on string columns, which is therefore marked informational when the collations differ rather than reported as a failure. `source_collation` is retained in canonical metadata so this warning can name the actual collation.

## 6. Date and time

| SQL Server | Canonical | Db2 target | Fidelity | Notes |
|---|---|---|---|---|
| `DATE` | `DATE` | `DATE` | LOSSLESS | Both 0001-01-01 to 9999-12-31 |
| `TIME(0)` | `TIME` | `TIME` | LOSSLESS | |
| `TIME(1..7)` | `TIME(n)` | policy | **REQUIRES POLICY** | Db2 `TIME` has no fractional seconds. `time_fraction_strategy`: `timestamp_anchored` (**recommended**) → `TIMESTAMP(n)` with date fixed to `0001-01-01`, lossless by convention; `varchar` → `VARCHAR(16)`, lossless, loses arithmetic; `truncate` → explicitly lossy, opt-in only; `fail` |
| `DATETIME` | `TIMESTAMP(3)` | `TIMESTAMP(3)` | LOSSLESS | 1/300 s ticks render to 3 decimal places; the stored millisecond value round-trips exactly |
| `SMALLDATETIME` | `TIMESTAMP(0)` | `TIMESTAMP(0)` | LOSSLESS | Minute granularity |
| `DATETIME2(n)`, n 0..7 | `TIMESTAMP(n)` | `TIMESTAMP(n)` | LOSSLESS | Db2 supports 0..12 |
| `DATETIMEOFFSET(n)` | `TIMESTAMP_TZ(n)` | policy | **REQUIRES POLICY** | `datetimeoffset_strategy`: `utc_plus_offset_column` (**default**) → `TIMESTAMP(n)` in UTC plus a `SMALLINT` sidecar `<col>_TZ_OFFSET_MIN`, fully lossless; `utc_only` → offset discarded, explicitly lossy; `varchar` → ISO-8601 text; `native_tz` → only if `TIMESTAMP WITH TIME ZONE` is **VERIFIED** on your Db2 LUW version, which IBM documents primarily for Db2 for z/OS |

`DATETIME`/`DATETIME2` carry **no** zone. DATMIG never applies an implicit timezone conversion to them; any conversion must be an explicit transformation rule.

## 7. Binary and unique identifiers

| SQL Server | Canonical | Db2 target | Fidelity | Notes |
|---|---|---|---|---|
| `UNIQUEIDENTIFIER` | `UUID` | `CHAR(36)` canonical text **default** | LOSSLESS (WIDENING strictly — `CHAR(36)` admits non-UUID strings) | Standard 8-4-4-4-12 form, uppercase to match SQL Server's rendering, applied identically on both sides. Alternative `CHAR(16) FOR BIT DATA` saves 20 bytes/row and indexes tighter but needs a fixed byte-order convention. `uuid_strategy: char36 \| binary16` |
| `BINARY(n)`, n ≤ 255 | `FIXED_BINARY(n)` | `CHAR(n) FOR BIT DATA` or `BINARY(n)` | LOSSLESS | Which form exists is Db2-version dependent — **VERIFY** |
| `VARBINARY(n)`, n ≤ 32672 | `BINARY(n)` | `VARCHAR(n) FOR BIT DATA` or `VARBINARY(n)` | LOSSLESS | Binary lengths need no expansion factor |
| `VARBINARY(n)`, n > 32672 | `BLOB` | `BLOB(n)` | LOSSLESS | Not reachable from a non-MAX SQL Server `VARBINARY`, whose ceiling is 8000 |
| `VARBINARY(MAX)` | `BLOB` | `BLOB(2G)` or downgrade | **REQUIRES POLICY** | Same `lob_strategy` as character LOBs |
| `IMAGE` | `BLOB` | `BLOB(2G)` | LOSSLESS | Deprecated in SQL Server |
| `ROWVERSION` / `TIMESTAMP` | `ROW_VERSION` | **excluded by default** | **REQUIRES POLICY** | A database-internal monotonic counter with no meaning in Db2. `rowversion_strategy`: `exclude` (**default**), `retain_as_binary` → `CHAR(8) FOR BIT DATA` for lineage, `fail`. Separately valuable as a change-detection key for a future `CDC_RECONCILED` mode — recorded in the plan even when excluded |

## 8. Structured and special types

| SQL Server | Canonical | Db2 target | Fidelity | Notes |
|---|---|---|---|---|
| `XML` | `XML` | Db2 `XML` if supported, else `CLOB` | **REQUIRES POLICY** | `xml_strategy: native \| clob \| fail`. Db2's XML type restricts some utilities and load paths, so `clob` is more predictable for bulk migration. Whitespace and attribute-order normalisation differ between engines, so byte-level digest comparison of XML columns is disabled unless `xml_strategy: clob` preserves the source bytes exactly |
| `sql_variant` | `UNSUPPORTED` | — | **UNSUPPORTED** | No Db2 equivalent. Exclude, or project to a concrete type with a custom transform |
| `hierarchyid` | `UNSUPPORTED` | — | **UNSUPPORTED** | Could become `VARBINARY`/`VARCHAR` via custom transform, losing all operator support |
| `geography` / `geometry` | `UNSUPPORTED` | — | **UNSUPPORTED** | Db2 Spatial Extender may apply — out of scope for v1 |
| `TABLE` / CLR UDTs | `UNSUPPORTED` | — | **UNSUPPORTED** | Out of scope |
| `sysname` | `STRING`, UTF16_CODE_UNITS, 128 | `VARCHAR(128 CODEUNITS32)` | WIDENING | Alias for `NVARCHAR(128)` |

## 9. Column attributes

| Attribute | Handling |
|---|---|
| **Nullability** | Preserved exactly. A source NOT NULL column stays NOT NULL unless a transformation introduces NULLs, in which case the plan fails rather than silently relaxing the constraint |
| **IDENTITY** | Target column created `GENERATED BY DEFAULT AS IDENTITY (START WITH <seed>, INCREMENT BY <increment>)` so source values insert directly. After the table completes, `ALTER TABLE ... ALTER COLUMN ... RESTART WITH <max+1>`, recorded as a `post_load_action` and audited |
| **DEFAULT** | Literal defaults translate directly. Expression defaults translate only from an allowlist (`getdate()` → `CURRENT TIMESTAMP`, `newid()` → policy). Untranslatable defaults are reported and omitted with a warning — never guessed |
| **Computed, PERSISTED** | Default: materialise as a plain column and copy the stored value. Optional `GENERATED ALWAYS AS (expr)` when the expression is in the translatable allowlist |
| **Computed, non-persisted** | Default: exclude with a warning — copying a derivable value creates drift |
| **Collation (column-level)** | Not reproduced at column level; retained in `source_collation` and surfaced as an informational plan warning |
| **Sparse columns / column sets** | Storage detail only; the logical column migrates normally |
| **Masked columns (Dynamic Data Masking)** | A masked column read without `UNMASK` returns masked data. Detected at discovery; **fails the plan** unless the policy explicitly acknowledges it. This is a silent-data-corruption trap and gets no benefit of the doubt |
| **Encrypted columns (Always Encrypted)** | Detected at discovery; requires explicit policy. Ciphertext can migrate as binary, but the target holds no key material |

## 10. Policy defaults

```yaml
type_policies:
  bit_strategy: smallint
  decimal_over_31: narrow_if_safe   # narrow_if_safe | decfloat | varchar | fail
  time_fraction_strategy: timestamp_anchored
  datetimeoffset_strategy: utc_plus_offset_column
  uuid_strategy: char36
  lob_strategy: downgrade_if_safe
  lob_downgrade_margin: 2.0
  rowversion_strategy: exclude
  xml_strategy: clob
  char_over_limit: varchar_padded

  # Character sizing (v0.2)
  string_units: octets              # octets | codeunits32
  size_from_profile: true           # size from measured MAX(DATALENGTH) when available
  profile_safety_margin: 1.5
  sbcs_to_utf8_bound: 3             # theoretical ceiling, used when profiling is unavailable
  nvarchar_to_utf8_bound: 3         # 3 octets per UTF-16 code unit is the true worst case

  accept_potentially_lossy: false
  accept_widening: true             # widening is normal and safe; set false to force review
```

Any column resolving to **REQUIRES POLICY** with no policy value, or to **POTENTIALLY LOSSY** with `accept_potentially_lossy: false` and no per-column override, produces a **blocking plan issue**. The plan cannot reach `READY`. **WIDENING** is accepted by default but is always itemised in the fidelity report, because "the target is wider than the source" is information a data owner is entitled to see before approving a migration.
