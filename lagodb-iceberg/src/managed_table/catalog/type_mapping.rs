//! Managed admission and primitive storage mapping over core's PostgreSQL type.

use iceberg_lite::spec::{PrimitiveType, Type};
use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationGuard;
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

use super::numeric_type::ManagedNumeric;
use crate::error::{IcebergError, IcebergResult};

pub(super) struct ManagedTypeMapping(PgType);

impl ManagedTypeMapping {
    pub(super) fn bind(pg_type: PgType) -> IcebergResult<Self> {
        let base = pg_type.base();
        // PG exposes element types for these vectors, but our list codecs
        // support ordinary arrays, not their distinct OIDs and zero bounds.
        // Like PG's array coercion path in parse_coerce.c, exclude them at the
        // capability boundary, without changing core's type metadata. Recursive
        // schema conversion reaches this check for nested types and domains.
        if matches!(base.oid, pg_sys::INT2VECTOROID | pg_sys::OIDVECTOROID) {
            let name = if base.oid == pg_sys::INT2VECTOROID {
                "int2vector"
            } else {
                "oidvector"
            };
            return Err(IcebergError::UnsupportedColumnType(name.into()));
        }
        if base.oid == pg_sys::NUMERICOID
            && ManagedNumeric::new(base.typmod).uses_double()
        {
            return Err(IcebergError::UnsupportedColumnType(
                "numeric without an Iceberg-compatible typmod must be normalized to double precision before managed schema creation".into(),
            ));
        }
        if base.is_composite() {
            // SAFETY: base.oid is a resolved catalog PgType, confirmed above
            // to be composite; this runs on the PostgreSQL backend thread.
            let relid = unsafe { pg_sys::get_typ_typrelid(base.oid) };
            // Keep the layout used to construct the Iceberg schema stable until
            // this column's dependency commits. Attribute add/drop/alter takes
            // AccessExclusiveLock on this same composite relation. Unlike the
            // dependency walker's inspection locks, this protects a new schema.
            let relation = RelationGuard::open_relation_retain_lock(
                relid,
                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
            )?;
            if relation.as_handle().relkind() as u8 != pg_sys::RELKIND_COMPOSITE_TYPE
            {
                return Err(IcebergError::UnsupportedColumnType(
                    "table row types are not supported as managed columns".into(),
                ));
            }
        }
        Ok(Self(base))
    }

    pub(super) fn pg_type(&self) -> PgType {
        self.0
    }

    /// Convert this type to an Iceberg `Type` when no field-id allocation is
    /// required (i.e. primitives only). Returns
    /// [`IcebergError::UnsupportedColumnType`] for arrays — those need a
    /// schema builder because the list-element field needs a fresh id.
    pub(super) fn primitive_type(&self) -> IcebergResult<Type> {
        let ty = self.0;
        if ty.is_object_id() {
            return Ok(Type::Primitive(PrimitiveType::Long));
        }
        let pg_oid = PgOid::from(self.0.oid);
        match pg_oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                Ok(Type::Primitive(PrimitiveType::Boolean))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2OID)
            | PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                Ok(Type::Primitive(PrimitiveType::Int))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                Ok(Type::Primitive(PrimitiveType::Long))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                Ok(Type::Primitive(PrimitiveType::Float))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                Ok(Type::Primitive(PrimitiveType::Double))
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                Ok(Type::Primitive(PrimitiveType::Date))
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
                Ok(Type::Primitive(PrimitiveType::Time))
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                Ok(Type::Primitive(PrimitiveType::Timestamp))
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                Ok(Type::Primitive(PrimitiveType::Timestamptz))
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
            | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
            | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID)
            | PgOid::BuiltIn(PgBuiltInOids::JSONOID)
            | PgOid::BuiltIn(PgBuiltInOids::JSONBOID)
            | PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => {
                Ok(Type::Primitive(PrimitiveType::String))
            }
            PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => {
                Ok(Type::Primitive(PrimitiveType::Binary))
            }
            PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => {
                Ok(Type::Primitive(PrimitiveType::Uuid))
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => self.decimal_type(),
            _ if self.supports_text_storage() => {
                if ty.is_enum() {
                    // ShareLock conflicts with RenameEnumLabel's ExclusiveLock.
                    // Hold it until the new column's dependency is committed.
                    unsafe {
                        pg_sys::LockDatabaseObject(
                            pg_sys::TypeRelationId,
                            ty.oid,
                            0,
                            pg_sys::ShareLock as pg_sys::LOCKMODE,
                        )
                    };
                }
                Ok(Type::Primitive(PrimitiveType::String))
            }
            _ => Err(IcebergError::UnsupportedColumnType(format!(
                "PostgreSQL type {} has no managed Iceberg storage representation",
                self.0.oid
            ))),
        }
    }

    fn decimal_type(&self) -> IcebergResult<Type> {
        Ok(Type::Primitive(
            ManagedNumeric::new(self.0.typmod).storage_type(),
        ))
    }

    /// Managed storage admission, not a capability claim by the generic I/O
    /// codec. Text encodings here are independent of session parsing settings;
    /// enums additionally use the managed DDL guard.
    fn supports_text_storage(&self) -> bool {
        if matches!(
            self.0.oid,
            pg_sys::INETOID
                | pg_sys::CIDROID
                | pg_sys::MACADDROID
                | pg_sys::MACADDR8OID
                | pg_sys::BITOID
                | pg_sys::VARBITOID
                | pg_sys::PG_LSNOID
                | pg_sys::TSVECTOROID
                | pg_sys::TSQUERYOID
        ) || self.0.is_enum()
        {
            return true;
        }
        // Support hstore through its text I/O contract. Resolve the
        // extension's actual type in its installation schema, not search_path.
        // Its other base type, ghstore, is an index key with no value I/O.
        unsafe {
            let extension = pg_sys::get_extension_oid(c"hstore".as_ptr(), true);
            if extension == pg_sys::InvalidOid {
                return false;
            }
            let namespace = pg_sys::get_extension_schema(extension);
            let hstore = pg_sys::GetSysCacheOid(
                pg_sys::SysCacheIdentifier::TYPENAMENSP as i32,
                pg_sys::Anum_pg_type_oid as pg_sys::AttrNumber,
                pg_sys::Datum::from(c"hstore".as_ptr()),
                pg_sys::Datum::from(namespace),
                pg_sys::Datum::from(0),
                pg_sys::Datum::from(0),
            );
            self.0.oid == hstore
                && pg_sys::getExtensionOfObject(pg_sys::TypeRelationId, hstore)
                    == extension
        }
    }
}
