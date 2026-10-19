//! Bound composite deformation/formation, following PostgreSQL rowtypes.c.

use pgrx::{PgTupleDesc, pg_sys};

use super::codec_context::CodecContext;
use super::pg_type::PgType;
use super::varlena::DetoastedVarlena;

/// Backend-local descriptor and reusable attribute buffers. Dropped attributes
/// remain NULL; consumers bind live attributes by their physical index.
pub struct CompositeDatumCodec {
    desc: PgTupleDesc<'static>,
    values: Vec<pg_sys::Datum>,
    nulls: Vec<bool>,
    _context: CodecContext,
}

impl CompositeDatumCodec {
    pub fn bind(pg_type: PgType) -> Self {
        let base = pg_type.base();
        let context = CodecContext::new(c"lagodb composite codec");
        let desc = context.with(|| unsafe {
            PgTupleDesc::from_pg_is_copy(pg_sys::lookup_rowtype_tupdesc_copy(
                base.oid,
                base.typmod,
            ))
        });
        let width = desc.len();
        Self {
            desc,
            values: vec![pg_sys::Datum::from(0); width],
            nulls: vec![true; width],
            _context: context,
        }
    }

    /// # Safety
    /// `datum` is a non-NULL composite of the bound type. The visitor must not
    /// retain any borrowed datum beyond this call.
    pub unsafe fn visit<R>(
        &mut self,
        datum: pg_sys::Datum,
        visit: impl FnOnce(&[pg_sys::Datum], &[bool]) -> R,
    ) -> R {
        let value = unsafe { DetoastedVarlena::from_datum(datum) };
        let header = value
            .as_datum()
            .cast_mut_ptr::<pg_sys::HeapTupleHeaderData>();
        let mut tuple = pg_sys::HeapTupleData {
            t_len: value.full_varlena_bytes().len() as u32,
            t_self: pg_sys::ItemPointerData::default(),
            t_tableOid: pg_sys::InvalidOid,
            t_data: header,
        };
        unsafe {
            pg_sys::heap_deform_tuple(
                &mut tuple,
                self.desc.as_ptr(),
                self.values.as_mut_ptr(),
                self.nulls.as_mut_ptr(),
            );
        }
        visit(&self.values, &self.nulls)
    }

    pub fn set(&mut self, index: usize, value: Option<pg_sys::Datum>) {
        self.nulls[index] = value.is_none();
        self.values[index] = value.unwrap_or(pg_sys::Datum::from(0));
    }

    /// # Safety
    /// All live attributes were set with datums of their declared type in the
    /// current row context. The returned composite is owned by that context.
    pub unsafe fn form(&mut self) -> pg_sys::Datum {
        let tuple = unsafe {
            pg_sys::heap_form_tuple(
                self.desc.as_ptr(),
                self.values.as_mut_ptr(),
                self.nulls.as_mut_ptr(),
            )
        };
        // HeapTupleGetDatum's implementation: heap_form_tuple already set the
        // composite header. Keep its allocation in the row context instead of
        // copying the complete tuple a second time.
        unsafe { pg_sys::HeapTupleHeaderGetDatum((*tuple).t_data) }
    }
}
