//! Native PostgreSQL-to-DataFusion scalar function identities.

use lagodb_core::expr::ExprType;
use pgrx::pg_sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarFunctionKind {
    Ascii,
    Repeat,
    StartsWith,
    Replace,
    CharacterLength,
    Substring,
    Reverse,
    Abs,
    Ceil,
    Floor,
    Greatest,
    Least,
    Coalesce,
    NullIf,
}

impl ScalarFunctionKind {
    pub(crate) const fn wire_id(self) -> i32 {
        self as i32 + 1
    }

    pub(crate) const fn from_wire_id(id: i32) -> Option<Self> {
        Some(match id {
            1 => Self::Ascii,
            2 => Self::Repeat,
            3 => Self::StartsWith,
            4 => Self::Replace,
            5 => Self::CharacterLength,
            6 => Self::Substring,
            7 => Self::Reverse,
            8 => Self::Abs,
            9 => Self::Ceil,
            10 => Self::Floor,
            11 => Self::Greatest,
            12 => Self::Least,
            13 => Self::Coalesce,
            14 => Self::NullIf,
            _ => return None,
        })
    }

    pub fn supports_signature(
        self,
        arguments: &[ExprType],
        input_collation: pg_sys::Oid,
        result: ExprType,
    ) -> bool {
        let has_oids = |expected: &[pg_sys::Oid]| {
            arguments.len() == expected.len()
                && arguments
                    .iter()
                    .zip(expected)
                    .all(|(argument, expected)| argument.type_oid == *expected)
        };
        let scalar_result = |oid| {
            result.type_oid == oid
                && result.typmod == -1
                && result.collation == pg_sys::InvalidOid
        };
        let text_result = || {
            result.type_oid == pg_sys::TEXTOID
                && result.typmod == -1
                && result.collation == input_collation
        };
        match self {
            Self::Ascii => {
                has_oids(&[pg_sys::TEXTOID]) && scalar_result(pg_sys::INT4OID)
            }
            Self::Repeat => {
                has_oids(&[pg_sys::TEXTOID, pg_sys::INT4OID]) && text_result()
            }
            Self::StartsWith => {
                has_oids(&[pg_sys::TEXTOID, pg_sys::TEXTOID])
                    && scalar_result(pg_sys::BOOLOID)
                    && Self::is_deterministic_collation(input_collation)
            }
            Self::Replace => {
                has_oids(&[pg_sys::TEXTOID, pg_sys::TEXTOID, pg_sys::TEXTOID])
                    && text_result()
                    && Self::is_deterministic_collation(input_collation)
            }
            Self::CharacterLength => {
                has_oids(&[pg_sys::TEXTOID]) && scalar_result(pg_sys::INT4OID)
            }
            Self::Substring => {
                matches!(arguments.len(), 2 | 3)
                    && arguments[0].type_oid == pg_sys::TEXTOID
                    && arguments[1..]
                        .iter()
                        .all(|argument| argument.type_oid == pg_sys::INT4OID)
                    && text_result()
            }
            Self::Reverse => has_oids(&[pg_sys::TEXTOID]) && text_result(),
            Self::Abs => {
                arguments.len() == 1
                    && matches!(
                        arguments[0].type_oid,
                        pg_sys::INT2OID
                            | pg_sys::INT4OID
                            | pg_sys::INT8OID
                            | pg_sys::FLOAT4OID
                            | pg_sys::FLOAT8OID
                    )
                    && scalar_result(arguments[0].type_oid)
            }
            Self::Ceil | Self::Floor => {
                has_oids(&[pg_sys::FLOAT8OID]) && scalar_result(pg_sys::FLOAT8OID)
            }
            Self::Greatest | Self::Least => {
                !arguments.is_empty()
                    && arguments.iter().all(|argument| {
                        matches!(
                            argument.type_oid,
                            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                        ) && argument.type_oid == result.type_oid
                    })
                    && scalar_result(result.type_oid)
            }
            Self::Coalesce => {
                !arguments.is_empty()
                    && arguments.iter().all(|argument| *argument == result)
                    && input_collation == result.collation
            }
            Self::NullIf => {
                arguments.len() == 2
                    && arguments[0] == result
                    && arguments[1] == result
                    && matches!(
                        result.type_oid,
                        pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
                    )
                    && result.typmod == -1
                    && result.collation == pg_sys::InvalidOid
            }
        }
    }

    fn is_deterministic_collation(collation: pg_sys::Oid) -> bool {
        collation != pg_sys::InvalidOid
            && unsafe { pg_sys::get_collation_isdeterministic(collation) }
    }
}
