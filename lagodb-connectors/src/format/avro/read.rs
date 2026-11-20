//! Schema-bound Avro object-container reading shared by FDW scans, COPY FROM,
//! and schema inference.
//!
//! apache-avro's `Reader` builds an owned `Value` tree for every record,
//! cloning each field name and allocating each string, byte value, and union.
//! This reader parses the writer schema once and compiles a plan from it. The
//! plan skips unprojected fields and decodes bound fields from the
//! decompressed block buffer, which every row of the block shares.

mod column;
mod decoder;
mod ocf;
mod plan;

use std::io::BufRead;

use apache_avro::Schema;
use apache_avro::schema::RecordSchema;

use crate::error::ConnectorError;
use crate::format::FormatKind;

pub(crate) use column::AvroReadColumn;
use decoder::{AvroCursor, AvroDatum};
use ocf::AvroContainer;
use plan::AvroReadPlan;

/// One object container whose header has been read.
pub(crate) struct AvroObjectHeader<R> {
    container: AvroContainer<R>,
    schema: Schema,
}

impl<R: BufRead> AvroObjectHeader<R> {
    pub(crate) fn read(input: R) -> Result<Self, ConnectorError> {
        let (container, schema) = AvroContainer::open(input)?;
        Ok(Self { container, schema })
    }

    /// The writer schema, which LagoDB reads only as a record of columns.
    pub(crate) fn record(&self) -> Result<&RecordSchema, ConnectorError> {
        match &self.schema {
            Schema::Record(record) => Ok(record),
            _ => Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "the Avro container writer schema must be a record",
            )),
        }
    }
}

/// Rows of one or more object containers that share a writer schema.
pub(crate) struct AvroObjectReader<R> {
    plan: AvroReadPlan,
    container: Option<AvroContainer<R>>,
    /// Block offset of each bound field in the current row.
    starts: Box<[usize]>,
}

impl<R: BufRead> AvroObjectReader<R> {
    /// Reads `header`'s object, decoding the writer fields at `bound`
    /// positions and skipping the others.
    pub(crate) fn new(
        header: AvroObjectHeader<R>,
        bound: impl IntoIterator<Item = usize>,
    ) -> Result<Self, ConnectorError> {
        let plan = AvroReadPlan::compile(header.schema, bound)?;
        let starts = vec![0; plan.field_count()].into_boxed_slice();
        Ok(Self {
            plan,
            container: Some(header.container),
            starts,
        })
    }

    /// Continues with the next object of the same prefix.
    pub(crate) fn open_next(
        &mut self,
        header: AvroObjectHeader<R>,
    ) -> Result<(), ConnectorError> {
        self.container = None;
        if header.schema != *self.plan.schema() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Avro,
                "objects under one prefix do not share the same Avro writer schema",
            ));
        }
        self.container = Some(header.container);
        Ok(())
    }

    /// Drops the current object; [`Self::next_row`] then returns `None` until
    /// [`Self::open_next`] supplies another.
    pub(crate) fn close(&mut self) {
        self.container = None;
    }

    /// Advances to the next row of the current object. An exhausted object
    /// keeps returning `None`.
    pub(crate) fn next_row(&mut self) -> Result<Option<AvroRow<'_>>, ConnectorError> {
        let Some(container) = self.container.as_mut() else {
            return Ok(None);
        };
        if !container.begin_row()? {
            return Ok(None);
        }
        let mut cursor = AvroCursor::new(container.block(), container.position());
        self.plan.scan_row(&mut cursor, &mut self.starts)?;
        let end = cursor.position();
        container.finish_row(end)?;
        Ok(Some(AvroRow {
            block: container.block(),
            plan: &self.plan,
            starts: &self.starts,
        }))
    }
}

/// One record whose structure has been checked; bound fields decode on access.
pub(crate) struct AvroRow<'r> {
    block: &'r [u8],
    plan: &'r AvroReadPlan,
    starts: &'r [usize],
}

impl<'r> AvroRow<'r> {
    fn datum(&self, field: usize) -> Result<AvroDatum<'r>, ConnectorError> {
        self.plan
            .decode_field(field, self.block, self.starts[field])
    }
}
