//! PostgreSQL COPY CSV-format object and its validated options.

use pg_lakebase_core::fdw::{
    ColumnRequirements, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyOperation, ForeignModifyPlanContext,
    ForeignModifyPlanSpec, ForeignModifyRelationContext, StartForeignScanContext,
};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::delimited::{DelimitedOptions, DelimitedOptionsBuilder};
use super::delimited_schema::DelimitedSchemaReader;
use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatScanPlanner,
    FormatScanState, FormatSchemaReader, FormatWritePrivate, FormatWriteState,
    FormatWriter, InferredSchema, StreamCompression,
};
use crate::fdw::LagodbConnectors;
use crate::storage::ObjectOutput;

/// CSV-format processor.
pub(crate) struct CsvFormat {
    pub(super) options: CsvOptions,
    pub(super) compression: StreamCompression,
}

#[derive(Debug)]
pub(super) struct CsvOptions {
    pub(super) delimited: DelimitedOptions,
    pub(super) quote: u8,
    pub(super) escape: u8,
    pub(super) header: CsvHeader,
}

impl CsvOptions {
    pub(super) fn postgres_options(
        &self,
        relation: &RelationHandle<'_>,
        requirements: &ColumnRequirements,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let mut options = self.delimited.append_postgres_options(
            std::ptr::null_mut(),
            FormatKind::Csv,
            relation,
            requirements,
        )?;
        options = self.append_postgres_quote_options(options)?;
        let header = match self.header {
            CsvHeader::False => "false",
            CsvHeader::True => "true",
            CsvHeader::Match => "match",
        };
        options = DelimitedOptions::append_string_option(options, "header", header)?;

        let mut force_null = Vec::new();
        let mut force_not_null = Vec::new();
        let columns = relation.live_columns();
        for column in columns.iter() {
            // SAFETY: PostgreSQL owns this foreign-column option list for the
            // live relation and attribute during this begin callback.
            let raw = unsafe {
                pg_sys::GetForeignColumnOptions(relation.oid(), column.attno())
            };
            let column_options = unsafe { ForeignOptionView::from_raw(raw) };
            let (is_force_null, is_force_not_null) =
                ColumnOptions::parse_view(column_options)?.flags();
            if is_force_null {
                force_null.push(column.name());
            }
            if is_force_not_null {
                force_not_null.push(column.name());
            }
        }
        if !force_null.is_empty() {
            options = DelimitedOptions::append_identifier_list_option(
                options,
                "force_null",
                force_null,
            )?;
        }
        if !force_not_null.is_empty() {
            options = DelimitedOptions::append_identifier_list_option(
                options,
                "force_not_null",
                force_not_null,
            )?;
        }
        Ok(options)
    }

    pub(super) fn postgres_output_options(
        &self,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let options = self
            .delimited
            .append_postgres_output_options(std::ptr::null_mut(), FormatKind::Csv)?;
        self.append_postgres_quote_options(options)
    }

    pub(super) fn postgres_schema_options(
        &self,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let options = self.postgres_output_options()?;
        DelimitedOptions::append_string_option(options, "header", "false")
    }

    pub(super) const fn header_enabled(&self) -> bool {
        matches!(self.header, CsvHeader::True | CsvHeader::Match)
    }

    fn append_postgres_quote_options(
        &self,
        mut options: *mut pg_sys::List,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        let quote = [self.quote];
        let quote = std::str::from_utf8(&quote).map_err(|_| {
            ConnectorError::invalid_option(
                "quote",
                "must be valid in the server encoding",
            )
        })?;
        options = DelimitedOptions::append_string_option(options, "quote", quote)?;
        let escape = [self.escape];
        let escape = std::str::from_utf8(&escape).map_err(|_| {
            ConnectorError::invalid_option(
                "escape",
                "must be valid in the server encoding",
            )
        })?;
        DelimitedOptions::append_string_option(options, "escape", escape)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CsvHeader {
    False,
    True,
    Match,
}

impl CsvHeader {
    fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("off") {
            Some(Self::False)
        } else if value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("on")
        {
            Some(Self::True)
        } else if value == "0" {
            Some(Self::False)
        } else if value.eq_ignore_ascii_case("match") {
            Some(Self::Match)
        } else {
            None
        }
    }
}

impl CsvFormat {
    pub(crate) fn resolve(
        compression: StreamCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        let mut delimited = DelimitedOptionsBuilder::default();
        let mut header = None;
        let mut quote = None;
        let mut escape = None;
        for option in options.iter().copied() {
            if delimited.consume(option)? {
                continue;
            }
            let target = match option.name() {
                "header" => &mut header,
                "quote" => &mut quote,
                "escape" => &mut escape,
                _ => {
                    return Err(ConnectorError::invalid_option(
                        option.name(),
                        "is not valid for csv",
                    ));
                }
            };
            if target.replace(option.value()).is_some() {
                return Err(ConnectorError::invalid_option(
                    option.name(),
                    "must not be specified more than once",
                ));
            }
        }

        let DelimitedOptions {
            delimiter,
            null_marker,
            encoding,
        } = delimited.resolve(",", "")?;
        let quote_value = quote.unwrap_or("\"");
        DelimitedOptionsBuilder::validate_single_byte("quote", quote_value)?;
        let quote = quote_value.as_bytes()[0];
        let escape_value = escape.unwrap_or(quote_value);
        DelimitedOptionsBuilder::validate_single_byte("escape", escape_value)?;
        let escape = escape_value.as_bytes()[0];
        if delimiter == quote {
            return Err(ConnectorError::invalid_option(
                "quote",
                "must differ from delimiter",
            ));
        }
        if null_marker.as_bytes().contains(&quote) {
            return Err(ConnectorError::invalid_option(
                "null",
                "cannot contain the CSV quote character",
            ));
        }
        let header = header
            .map(|value| {
                CsvHeader::parse(value).ok_or_else(|| {
                    ConnectorError::invalid_option(
                        "header",
                        "must be false, true, on, off, 0, 1, or match",
                    )
                })
            })
            .transpose()?
            .unwrap_or(CsvHeader::False);

        Ok(Self {
            options: CsvOptions {
                delimited: DelimitedOptions {
                    delimiter,
                    null_marker,
                    encoding,
                },
                quote,
                escape,
                header,
            },
            compression,
        })
    }

    pub(crate) fn validate_column_options(
        options: &[Option<String>],
    ) -> Result<(), ConnectorError> {
        ColumnOptions::parse(options)?.validate()
    }

    pub(crate) fn validate_column_view(
        options: ForeignOptionView<'_>,
    ) -> Result<(), ConnectorError> {
        ColumnOptions::parse_view(options)?.validate()
    }
}

impl FormatObject for CsvFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Csv
    }
}

impl FormatSchemaReader for CsvFormat {
    fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        // SAFETY: postgres_schema_options returns a PostgreSQL-owned COPY
        // option list in the current context, which outlives inference.
        unsafe {
            DelimitedSchemaReader::new(
                FormatKind::Csv,
                self.options.header_enabled(),
                self.options.postgres_schema_options()?,
            )
        }
        .infer(file, self.compression)
    }
}

impl FormatReader for CsvFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(super::delimited_scan::DelimitedScanPlanner::new(
            FormatKind::Csv,
        ))
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: crate::storage::ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options =
            options.postgres_options(&context.relation, context.required_columns)?;
        Ok(Box::new(super::delimited_scan::DelimitedScanState::begin(
            context,
            files,
            compression,
            postgres_options,
        )?))
    }
}

impl FormatWriter for CsvFormat {
    fn capabilities(
        &self,
        _context: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ConnectorError> {
        Ok(ForeignModifyCapabilities::new(true, false, false))
    }

    fn plan_modify(
        &self,
        context: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<FormatWritePrivate>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Csv));
        }
        Ok(ForeignModifyPlanSpec::new(FormatWritePrivate::new(
            FormatKind::Csv,
        )))
    }

    fn begin_modify(
        self: Box<Self>,
        context: ForeignModifyBeginContext<'_, FormatWritePrivate>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Csv));
        }
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options = options.postgres_output_options()?;
        let write_header = options.header_enabled();
        Ok(Box::new(
            super::delimited_write::DelimitedWriteState::begin(
                context.relation(),
                output,
                FormatKind::Csv,
                compression,
                postgres_options,
                write_header,
            )?,
        ))
    }

    fn begin_insert(
        self: Box<Self>,
        context: &mut ForeignInsertBeginContext<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options = options.postgres_output_options()?;
        let write_header = options.header_enabled();
        Ok(Box::new(
            super::delimited_write::DelimitedWriteState::begin(
                context.relation(),
                output,
                FormatKind::Csv,
                compression,
                postgres_options,
                write_header,
            )?,
        ))
    }
}

struct ColumnOptions {
    force_null: bool,
    force_not_null: bool,
    seen_force_null: bool,
    seen_force_not_null: bool,
}

impl ColumnOptions {
    fn empty() -> Self {
        Self {
            force_null: false,
            force_not_null: false,
            seen_force_null: false,
            seen_force_not_null: false,
        }
    }

    fn parse(options: &[Option<String>]) -> Result<Self, ConnectorError> {
        let mut parsed = Self::empty();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                ConnectorError::invalid_option(
                    "foreign column option",
                    "expected name=value",
                )
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn parse_view(options: ForeignOptionView<'_>) -> Result<Self, ConnectorError> {
        let mut parsed = Self::empty();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                ConnectorError::invalid_option(
                    "foreign column option",
                    "must be valid UTF-8",
                )
            })?;
            let value = option.value_str().map_err(|_| {
                ConnectorError::invalid_option(name, "must be valid UTF-8")
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn set(&mut self, name: &str, value: &str) -> Result<(), ConnectorError> {
        let (seen, target) = match name {
            "force_null" => (&mut self.seen_force_null, &mut self.force_null),
            "force_not_null" => {
                (&mut self.seen_force_not_null, &mut self.force_not_null)
            }
            _ => {
                return Err(ConnectorError::invalid_option(
                    name,
                    "is not a supported foreign column option",
                ));
            }
        };
        if *seen {
            return Err(ConnectorError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        *seen = true;
        *target = Self::parse_boolean(name, value)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.force_null && self.force_not_null {
            return Err(ConnectorError::invalid_option(
                "force_null/force_not_null",
                "cannot both be true",
            ));
        }
        Ok(())
    }

    fn flags(&self) -> (bool, bool) {
        (self.force_null, self.force_not_null)
    }

    fn parse_boolean(name: &str, value: &str) -> Result<bool, ConnectorError> {
        if value == "1"
            || value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("on")
        {
            Ok(true)
        } else if value == "0"
            || value.eq_ignore_ascii_case("false")
            || value.eq_ignore_ascii_case("off")
        {
            Ok(false)
        } else {
            Err(ConnectorError::invalid_option(
                name,
                "must be true, false, on, off, 1, or 0",
            ))
        }
    }
}
