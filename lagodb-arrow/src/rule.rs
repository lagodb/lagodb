//! Dispatch data models and rule resolution.

use arrow_schema::{DataType, FieldRef, TimeUnit};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

use crate::error::{ArrowConversionError, ArrowConversionResult};

/// The target PostgreSQL column type, classified into the coarse bucket the
/// `(Arrow DataType, PG type)` dispatch keys on.
///
/// Supplied by the consumer from the relation's `TupleDesc` (see
/// [`Self::from_pg_type`]). [`resolve_cell_rule`] checks every column's
/// Arrow type against this target, so it is load-bearing for *all* columns —
/// it both rejects incompatible pairs (e.g. `Int32` data aimed at a `text`
/// column) and makes the one distinction the Arrow type cannot make on its
/// own: telling a `uuid` column apart from a fixed-width `bytea` column when
/// both materialize as `FixedSizeBinary(16)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgColumnType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Bytea,
    Jsonb,
    Uuid,
    Numeric,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    /// A one-dimensional array, carrying the **target element type OID** (the
    /// array's element OID from `pg_type.typelem`). The element OID is
    /// load-bearing for read-side datum construction: a single Arrow physical
    /// element type backs several PG element types (`Int32` backs `int2`/`int4`;
    /// `Utf8` backs `text`/`varchar`/`bpchar`/`name`/`json`), and the produced
    /// array Datum must carry the column's *declared* element OID — and narrow
    /// the value to it — rather than one reverse-engineered from the Arrow
    /// element. The element kind still comes from the Arrow list element; this
    /// OID disambiguates the target the same way the scalar buckets do.
    Array(pg_sys::Oid),
}

impl PgColumnType {
    /// Classify a PostgreSQL column's type OID into the coarse target bucket the
    /// `(Arrow DataType, PG type)` dispatch keys on.
    ///
    /// This is the inverse-direction companion to a consumer's PG→format schema
    /// mapping: the consumer hands the slot attribute's *real* OID (from its
    /// `TupleDesc`) and this layer decides which bucket it falls in, so rule
    /// resolution validates against the column's actual type rather than a value
    /// round-tripped back from the table format. Returns `None` for an OID this
    /// layer cannot target.
    ///
    /// Several OIDs collapse to one bucket on purpose: `text`/`varchar`/
    /// `bpchar`/`name`/`json` → [`Text`](Self::Text). `jsonb` remains distinct
    /// as a PostgreSQL target, but this format-neutral resolver does not choose
    /// a physical representation for it; a provider must bind that explicitly.
    /// An array OID collapses to [`Array`](Self::Array)
    /// carrying its **element** OID (`pg_type.typelem`); the element *kind*
    /// comes from the Arrow list element, but the element OID is what read-side
    /// datum construction targets (and narrows to).
    pub fn from_pg_type(oid: pg_sys::Oid) -> Option<Self> {
        let bucket = match PgOid::from(oid) {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => Self::Bool,
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => Self::Int2,
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => Self::Int4,
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => Self::Int8,
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => Self::Float4,
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => Self::Float8,
            PgOid::BuiltIn(
                PgBuiltInOids::TEXTOID
                | PgBuiltInOids::VARCHAROID
                | PgBuiltInOids::BPCHAROID
                | PgBuiltInOids::NAMEOID
                | PgBuiltInOids::JSONOID,
            ) => Self::Text,
            PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => Self::Bytea,
            PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => Self::Jsonb,
            PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => Self::Uuid,
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => Self::Numeric,
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => Self::Date,
            PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => Self::Time,
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => Self::Timestamp,
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => Self::Timestamptz,
            _ => {
                // Array types have no fixed builtin-OID set to match against, so
                // ask PostgreSQL whether this OID is a (varlena) array type.
                // SAFETY: `get_element_type` only reads the `pg_type` syscache
                // and returns `InvalidOid` for any non-array OID.
                let element = unsafe { pg_sys::get_element_type(oid) };
                if element != pg_sys::InvalidOid {
                    Self::Array(element)
                } else {
                    return None;
                }
            }
        };
        Some(bucket)
    }
}

/// Conversion vocabulary for the owned `Cell` subset. Declared PostgreSQL
/// containers, text I/O and nested JSONB belong to
/// [`BoundDatumPlan`](crate::BoundDatumPlan), which reuses these leaf rules.
#[derive(Debug, Clone)]
pub enum CellRule {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    /// `Utf8` or `LargeUtf8`.
    Utf8,
    /// `Binary` or `LargeBinary`.
    Binary,
    /// `FixedSizeBinary(len)` mapped to `bytea`.
    FixedBinary {
        len: usize,
    },
    /// `FixedSizeBinary(16)` mapped to `uuid`.
    Uuid,
    Date32,
    Time64Micros,
    /// `nanos` selects the nanosecond physical unit; `tz` selects a tz-aware
    /// (`+00:00`) column over a tz-naive one.
    Timestamp {
        nanos: bool,
        tz: bool,
    },
    Decimal128 {
        precision: u32,
        scale: u32,
    },
    /// A single-level list. `field` is the ready Arrow element `Field`
    /// (carrying any field-id metadata) supplied by the consumer's schema, so
    /// the build path can emit a `ListArray` without consulting any
    /// table-format constant. `elem_oid` is the **target** PostgreSQL element
    /// type OID the read path materializes into (and narrows to); the write
    /// path ignores it (the element kind comes from the live datum's array).
    List {
        element: ListElementRule,
        field: FieldRef,
        elem_oid: pg_sys::Oid,
    },
}

impl CellRule {
    /// Whether an Arrow array of `dt` can be decoded by this already-resolved
    /// rule.
    ///
    /// This is the batch-boundary check that complements
    /// [`crate::column_encoder::downcast`]: `downcast` only confirms the *concrete*
    /// array kind, but several Arrow types share a kind while differing in the
    /// `DataType` parameters the value math depends on — decimal
    /// precision/scale, fixed-binary width, timestamp unit/timezone. Validating
    /// the full `DataType` once per scan (against the type the plan resolved
    /// from the schema) turns a producer/plan drift into a clean
    /// [`ArrowConversionError::ArrowTypeMismatch`] at the boundary instead of a panic or a
    /// value silently decoded at the wrong scale/width.
    ///
    /// The accepted set mirrors [`resolve_cell_rule`] (e.g. `Utf8` accepts
    /// `Utf8`/`LargeUtf8`) and the exact unit the decoder reads for timestamps.
    pub fn accepts(&self, dt: &DataType) -> bool {
        match self {
            CellRule::Bool => matches!(dt, DataType::Boolean),
            CellRule::I16 => matches!(dt, DataType::Int16),
            CellRule::I32 => matches!(dt, DataType::Int32),
            CellRule::I64 => matches!(dt, DataType::Int64),
            CellRule::F32 => matches!(dt, DataType::Float32),
            CellRule::F64 => matches!(dt, DataType::Float64),
            CellRule::Utf8 => matches!(dt, DataType::Utf8 | DataType::LargeUtf8),
            CellRule::Binary => {
                matches!(dt, DataType::Binary | DataType::LargeBinary)
            }
            CellRule::FixedBinary { len } => {
                matches!(dt, DataType::FixedSizeBinary(n) if *n as usize == *len)
            }
            CellRule::Uuid => matches!(dt, DataType::FixedSizeBinary(16)),
            CellRule::Date32 => matches!(dt, DataType::Date32),
            CellRule::Time64Micros => {
                matches!(dt, DataType::Time64(TimeUnit::Microsecond))
            }
            CellRule::Timestamp { nanos, tz } => {
                let want_unit = if *nanos {
                    TimeUnit::Nanosecond
                } else {
                    TimeUnit::Microsecond
                };
                matches!(
                    dt,
                    DataType::Timestamp(unit, zone)
                        if *unit == want_unit && zone.is_some() == *tz
                )
            }
            CellRule::Decimal128 { precision, scale } => matches!(
                dt,
                DataType::Decimal128(p, s)
                    if *p as u32 == *precision && *s as u32 == *scale
            ),
            CellRule::List { element, .. } => matches!(
                dt,
                DataType::List(field)
                    if element.accepts_data_type(field.data_type())
            ),
        }
    }

    /// Whether decoding or encoding this rule crosses the PostgreSQL/Arrow
    /// semantic UTF-8 boundary.
    pub(crate) fn requires_utf8_server_encoding(&self) -> bool {
        matches!(self, Self::Utf8)
            || matches!(
                self,
                Self::List {
                    element: ListElementRule::String,
                    ..
                }
            )
    }
}

/// The element kind of a supported list column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListElementRule {
    Bool,
    /// Native `Int16`, preserving smallint's value domain in external storage.
    Short,
    /// `Int32`, including provider-controlled widening of PG int2[].
    Int,
    /// `Int64`.
    Long,
    /// `Float32`.
    Float,
    /// `Float64`.
    Double,
    /// `Utf8` or `LargeUtf8`.
    String,
}

impl ListElementRule {
    /// Classify the element families with an existing specialized list codec.
    pub(crate) fn for_arrow_type(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Boolean => Some(Self::Bool),
            DataType::Int16 => Some(Self::Short),
            DataType::Int32 => Some(Self::Int),
            DataType::Int64 => Some(Self::Long),
            DataType::Float32 => Some(Self::Float),
            DataType::Float64 => Some(Self::Double),
            DataType::Utf8 | DataType::LargeUtf8 => Some(Self::String),
            _ => None,
        }
    }

    /// Whether an Arrow list-element `DataType` can be decoded by this element
    /// rule — the element-level counterpart of [`CellRule::accepts`], used to
    /// validate a list column's element type at the batch boundary.
    ///
    /// This is kept in lockstep with the physical types `read::list` handles:
    /// `String` accepts `Utf8`/`LargeUtf8`, and every other kind matches exactly. Element
    /// types the resolver rejects (e.g. `Binary`, `UInt32`, a nested `List`)
    /// therefore fail here rather than slipping through as "some list".
    pub(crate) fn accepts_data_type(&self, dt: &DataType) -> bool {
        match self {
            ListElementRule::Bool => matches!(dt, DataType::Boolean),
            ListElementRule::Short => matches!(dt, DataType::Int16),
            ListElementRule::Int => matches!(dt, DataType::Int32),
            ListElementRule::Long => matches!(dt, DataType::Int64),
            ListElementRule::Float => matches!(dt, DataType::Float32),
            ListElementRule::Double => matches!(dt, DataType::Float64),
            ListElementRule::String => {
                matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
            }
        }
    }

    /// Whether this element kind can materialize into the array column's
    /// declared **element** type OID — the element-level counterpart of the
    /// scalar `(Arrow DataType, PgColumnType)` compatibility decided in
    /// [`resolve_cell_rule`]. Mirrors those scalar arms exactly: an `Int`
    /// element backs `int2`/`int4` (narrowed at datum construction), `String`
    /// backs the `text`/`varchar`/`bpchar`/`name`/`json` family, and every other
    /// kind targets its single canonical OID.
    pub(crate) fn accepts_target_oid(&self, oid: pg_sys::Oid) -> bool {
        use PgBuiltInOids as B;
        let PgOid::BuiltIn(builtin) = PgOid::from(oid) else {
            return false;
        };
        match self {
            ListElementRule::Bool => builtin == B::BOOLOID,
            ListElementRule::Short => builtin == B::INT2OID,
            ListElementRule::Int => matches!(builtin, B::INT2OID | B::INT4OID),
            ListElementRule::Long => builtin == B::INT8OID,
            ListElementRule::Float => builtin == B::FLOAT4OID,
            ListElementRule::Double => builtin == B::FLOAT8OID,
            ListElementRule::String => matches!(
                builtin,
                B::TEXTOID | B::VARCHAROID | B::BPCHAROID | B::NAMEOID | B::JSONOID
            ),
        }
    }
}

/// Resolve the conversion rule for a column from its Arrow `DataType` and
/// target PostgreSQL type.
///
/// The pair must be one this layer can materialize: each recognized Arrow type
/// admits a specific set of PostgreSQL target buckets (e.g. `Int32` backs both
/// `int2` and `int4`, since both round-trip through a 32-bit Arrow column;
/// `Timestamp` with a zone backs `timestamptz`, without a zone backs
/// `timestamp`). This is the single point where `(Arrow, PG)` compatibility is
/// decided, so a desync such as `Int32` data aimed at a `text` column is
/// rejected here at converter construction rather than surfacing as a corrupt
/// or failed datum mid-scan.
///
/// The admitted pairs serve both directions: [`BoundDatumPlan`] reads (Arrow to
/// PostgreSQL) and writes (PostgreSQL to Arrow) resolve through this function,
/// because a table column binding must round-trip. UPDATE reads and rewrites
/// whole rows, and a transaction reads its own writes. Every admitted pair
/// therefore has both a decoder and a source-bound encoder. Three pairs are
/// lossless only when written, so providers guard the read direction:
/// `Int32` to `int2` relies on the producer preserving the int16 domain;
/// nanosecond timestamps truncate to microseconds (foreign UPDATE rejects
/// them); and `Decimal128` to a typmod-constrained `numeric` is checked per
/// value by the target typmod. Do not add a pair that holds in only one
/// direction here; that would need a direction-specific resolver.
///
/// [`BoundDatumPlan`]: crate::BoundDatumPlan
///
/// Returns [`ArrowConversionError::IncompatibleColumnType`] when the `DataType` is
/// recognized but paired with an incompatible PostgreSQL type, and
/// [`ArrowConversionError::UnsupportedColumnType`] when the `DataType` is one this layer
/// cannot materialize at all.
pub fn resolve_cell_rule(
    arrow: &DataType,
    pg: PgColumnType,
) -> ArrowConversionResult<CellRule> {
    use PgColumnType as Pg;
    match arrow {
        DataType::Boolean => require_pg(pg == Pg::Bool, arrow, pg, CellRule::Bool),
        DataType::Int16 => require_pg(pg == Pg::Int2, arrow, pg, CellRule::I16),
        // Iceberg has no int16. Providers using Int32 for int2 must preserve
        // the original PG value domain; generic external Int32 is not proof.
        DataType::Int32 => {
            require_pg(matches!(pg, Pg::Int2 | Pg::Int4), arrow, pg, CellRule::I32)
        }
        DataType::Int64 => require_pg(pg == Pg::Int8, arrow, pg, CellRule::I64),
        DataType::Float32 => require_pg(pg == Pg::Float4, arrow, pg, CellRule::F32),
        DataType::Float64 => require_pg(pg == Pg::Float8, arrow, pg, CellRule::F64),
        DataType::Utf8 | DataType::LargeUtf8 => {
            require_pg(pg == Pg::Text, arrow, pg, CellRule::Utf8)
        }
        DataType::Binary | DataType::LargeBinary => {
            require_pg(pg == Pg::Bytea, arrow, pg, CellRule::Binary)
        }
        DataType::Date32 => require_pg(pg == Pg::Date, arrow, pg, CellRule::Date32),
        DataType::Time64(TimeUnit::Microsecond) => {
            require_pg(pg == Pg::Time, arrow, pg, CellRule::Time64Micros)
        }
        DataType::Timestamp(unit, tz) => {
            // Only microsecond and nanosecond physical units are materializable
            // (the decoder downcasts to exactly those, and `accepts` admits only
            // them). Reject second/millisecond here — at the single (Arrow, PG)
            // compatibility point — so `validate_cell_schema` fails at the schema
            // boundary instead of letting an unsupported unit pass the gate and
            // surface only on the first decoded batch.
            let nanos = match unit {
                TimeUnit::Microsecond => false,
                TimeUnit::Nanosecond => true,
                TimeUnit::Second | TimeUnit::Millisecond => {
                    return Err(ArrowConversionError::UnsupportedColumnType(
                        format!(
                            "Timestamp unit {unit:?} is not supported \
                         (only microsecond and nanosecond)"
                        ),
                    ));
                }
            };
            // A zone-aware Arrow timestamp must land in `timestamptz` and a
            // zone-naive one in `timestamp`; the physical unit is independent.
            let want = if tz.is_some() {
                Pg::Timestamptz
            } else {
                Pg::Timestamp
            };
            require_pg(
                pg == want,
                arrow,
                pg,
                CellRule::Timestamp {
                    nanos,
                    tz: tz.is_some(),
                },
            )
        }
        DataType::Decimal128(precision, scale) => {
            if pg != Pg::Numeric {
                return Err(incompatible(arrow, pg));
            }
            // `1 <= precision <= 38` bounds the i128 range the encoder needs
            // (it computes `10^precision`/`10^scale` as `i128`, which holds iff
            // the exponent is ≤ 38). Iceberg, Arrow and PostgreSQL all require
            // `0 <= scale <= precision` as a *schema* invariant, which both
            // executors enforce later (read: `Decimal128NumericCodec::new`;
            // write: `with_precision_and_scale` at flush). Resolve it here too
            // so an illegal external schema fails at session begin — "resolve
            // once, fail early" — instead of only on the first decode/flush.
            // (Per-*value* over-precision stays a separate runtime check at
            // encode time, not here.) The third clause short-circuits after the
            // first two, so `scale` is already in `0..=38` and the cast is safe.
            if !(1..=38).contains(precision)
                || !(0..=38).contains(scale)
                || *scale as u32 > *precision as u32
            {
                return Err(ArrowConversionError::IncompatibleColumnType(
                    format!("Decimal128({precision}, {scale})"),
                    "require 1 <= precision <= 38 and 0 <= scale <= precision"
                        .to_string(),
                ));
            }
            Ok(CellRule::Decimal128 {
                precision: *precision as u32,
                scale: *scale as u32,
            })
        }
        // UUID and fixed-width bytea both arrive as FixedSizeBinary; the target
        // PostgreSQL type decides which rule applies.
        DataType::FixedSizeBinary(n) => {
            let n = *n;
            if n == 16 && pg == Pg::Uuid {
                Ok(CellRule::Uuid)
            } else if pg == Pg::Bytea && (1..=i32::MAX).contains(&n) {
                Ok(CellRule::FixedBinary { len: n as usize })
            } else if n == 16 {
                Err(ArrowConversionError::IncompatibleColumnType(
                    "FixedSizeBinary(16)".to_string(),
                    format!("target PG type {pg:?} is neither uuid nor bytea"),
                ))
            } else {
                Err(ArrowConversionError::UnsupportedColumnType(format!(
                    "FixedSizeBinary({n}) is only supported as bytea"
                )))
            }
        }
        DataType::List(element_field) => {
            let Pg::Array(elem_oid) = pg else {
                return Err(incompatible(arrow, pg));
            };
            let element = resolve_list_element_rule(element_field.data_type())?;
            // The element-granularity twin of the scalar arms' single-point
            // (Arrow, PG) compatibility check: the resolved element kind must be
            // materializable into the column's declared element OID (an `Int32`
            // element backs `int2`/`int4`, a `Utf8` element backs the text
            // family), so a desync is rejected here rather than mid-scan at datum
            // construction.
            if !element.accepts_target_oid(elem_oid) {
                return Err(ArrowConversionError::IncompatibleColumnType(
                    format!("{arrow:?}"),
                    format!(
                        "list element rule {element:?} cannot materialize into PG \
                         element OID {}",
                        u32::from(elem_oid)
                    ),
                ));
            }
            Ok(CellRule::List {
                element,
                field: element_field.clone(),
                elem_oid,
            })
        }
        other => Err(ArrowConversionError::UnsupportedColumnType(format!(
            "{other:?}"
        ))),
    }
}

/// Return `rule` when the resolved `(arrow, pg)` pair is compatible, otherwise
/// the [`ArrowConversionError::IncompatibleColumnType`] naming the offending pair. Keeps
/// the per-`DataType` arms above to a single line each.
fn require_pg(
    compatible: bool,
    arrow: &DataType,
    pg: PgColumnType,
    rule: CellRule,
) -> ArrowConversionResult<CellRule> {
    if compatible {
        Ok(rule)
    } else {
        Err(incompatible(arrow, pg))
    }
}

/// Build the [`ArrowConversionError::IncompatibleColumnType`] for a recognized Arrow type
/// paired with a PostgreSQL type it cannot target.
fn incompatible(arrow: &DataType, pg: PgColumnType) -> ArrowConversionError {
    ArrowConversionError::IncompatibleColumnType(
        format!("{arrow:?}"),
        format!("cannot materialize into PG type {pg:?}"),
    )
}

/// Resolve the conversion rule for a list element from its Arrow `DataType`.
/// Anything outside `bool`/`int`/`long`/`float`/`double`/`string` (including a
/// nested list) is rejected with [`ArrowConversionError::UnsupportedColumnType`].
pub fn resolve_list_element_rule(
    element_type: &DataType,
) -> ArrowConversionResult<ListElementRule> {
    ListElementRule::for_arrow_type(element_type).ok_or_else(|| {
        ArrowConversionError::UnsupportedColumnType(format!(
            "list element type {element_type:?} is not supported"
        ))
    })
}

/// Reject any column whose `(DataType, PgColumnType)` pair cannot be
/// materialized, in ascending column order. `pg_column_types[i]` is the target
/// type for `arrow_schema.field(i)`, so the two views must describe the same
/// columns and have equal length.
///
/// This is a pure type-level check — it never reads a row or array value — and
/// returns `Ok(())` for a zero-column schema. It is the schema-granularity
/// twin of [`resolve_cell_rule`]: it resolves each column's rule and discards
/// the result, so the two stay in lockstep by sharing the same resolver. A
/// length mismatch between the two views is a caller contract violation (the
/// Arrow schema and the PG target-type list must align column-for-column) and
/// is reported rather than silently truncated to the shorter view.
pub fn validate_cell_schema(
    arrow_schema: &arrow_schema::Schema,
    pg_column_types: &[PgColumnType],
) -> ArrowConversionResult<()> {
    let fields = arrow_schema.fields();
    if fields.len() != pg_column_types.len() {
        return Err(ArrowConversionError::InvariantViolated(
            "validate_cell_schema: Arrow schema and PG target-type counts differ",
        ));
    }
    for (field, pg) in fields.iter().zip(pg_column_types) {
        resolve_cell_rule(field.data_type(), *pg)?;
    }
    Ok(())
}
