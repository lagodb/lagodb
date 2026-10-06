#include "postgres.h"

#include "lagodb_copy.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/miscnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "miscadmin.h"
#include "tcop/utility.h"

#include "lagodb_relation.h"

#if !LAGODB_PG17
#error "COPY bridge has not been ported to this PostgreSQL major version"
#endif

/*
 * The row encoder owns its state and uses the source-derived text/CSV
 * serializer from the audited PG17.0-PG17.10 epoch. Keep minor-version
 * branches local to this code when a future audit finds a relevant
 * options, output-function, or serializer contract change.
 */

CopyFromState
lagodb_begin_copy_from(ParseState *pstate,
					   Relation rel,
					   Node *where_clause,
					   const char *filename,
					   bool is_program,
					   copy_data_source_cb data_source_cb,
					   List *attnamelist,
					   List *options)
{
	return BeginCopyFrom(pstate,
						 rel,
						 where_clause,
						 filename,
						 is_program,
						 data_source_cb,
						 attnamelist,
						 options);
}

bool
lagodb_next_copy_from(CopyFromState state,
					  ExprContext *econtext,
					  Datum *values,
					  bool *nulls)
{
	ErrorContextCallback errcallback;
	MemoryContext oldcontext;
	bool		found = false;

	/* NextCopyFrom reports parser/type errors through this callback. */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* DEFAULT expressions are evaluated in PostgreSQL's per-tuple context. */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	PG_TRY();
	{
		found = NextCopyFrom(state, econtext, values, nulls);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		error_context_stack = errcallback.previous;
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	error_context_stack = errcallback.previous;
	return found;
}

void
lagodb_end_copy_from(CopyFromState state)
{
	EndCopyFrom(state);
}

struct LagodbRawFieldReader
{
	MemoryContext context;
	CopyFromState state;
};

struct LagodbTextInputValidator
{
	MemoryContext context;
	MemoryContext call_context;
	FmgrInfo	input_function;
	Oid			typioparam;
};

/*
 * BeginCopyFrom requires a Relation for parser metadata even when callers use
 * only NextCopyFromRawFields. This descriptor is intentionally one text
 * column: the raw-field parser grows its internal field array for wider rows.
 * It lives below the reader context and is never exposed to executor code.
 */
static Relation
lagodb_raw_fields_relation(void)
{
	Relation	relation = palloc0(sizeof(RelationData));
	TupleDesc	descriptor = CreateTemplateTupleDesc(1);

	TupleDescInitEntry(descriptor, 1, "column1", TEXTOID, -1, 0);
	relation->rd_att = descriptor;
	relation->rd_rel = palloc0(sizeof(FormData_pg_class));
	namestrcpy(&relation->rd_rel->relname, "lagodb_schema_inference");
	return relation;
}

LagodbRawFieldReader *
lagodb_begin_raw_field_reader(copy_data_source_cb data_source_cb, List *options)
{
	LagodbRawFieldReader *reader;
	MemoryContext context;
	MemoryContext oldcontext;

	context = AllocSetContextCreate(CurrentMemoryContext,
									"LagoDB raw COPY fields",
									ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(context);
	PG_TRY();
	{
		reader = palloc0(sizeof(LagodbRawFieldReader));
		reader->context = context;
		reader->state = BeginCopyFrom(NULL, lagodb_raw_fields_relation(),
									  NULL, NULL, false, data_source_cb,
									  NIL, options);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	return reader;
}

bool
lagodb_next_raw_fields(LagodbRawFieldReader *reader, char ***fields,
					   size_t *field_count)
{
	ErrorContextCallback errcallback;
	int			nfields;
	bool		found;

	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = reader->state;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;
	PG_TRY();
	{
		found = NextCopyFromRawFields(reader->state, fields, &nfields);
	}
	PG_CATCH();
	{
		error_context_stack = errcallback.previous;
		PG_RE_THROW();
	}
	PG_END_TRY();
	error_context_stack = errcallback.previous;
	if (!found)
		return false;

	/* PostgreSQL's public raw-field API reports a nonnegative field count. */
	Assert(nfields >= 0);
	*field_count = (size_t) nfields;
	return true;
}

void
lagodb_end_raw_field_reader(LagodbRawFieldReader *reader)
{
	EndCopyFrom(reader->state);
	MemoryContextDelete(reader->context);
}

LagodbTextInputValidator *
lagodb_begin_text_input_validator(Oid type_oid)
{
	LagodbTextInputValidator *validator;
	MemoryContext context;
	MemoryContext oldcontext;
	Oid			input_function;

	context = AllocSetContextCreate(CurrentMemoryContext,
									"LagoDB text input validator",
									ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(context);
	PG_TRY();
	{
		validator = palloc0(sizeof(LagodbTextInputValidator));
		validator->context = context;
		getTypeInputInfo(type_oid, &input_function, &validator->typioparam);
		fmgr_info_cxt(input_function, &validator->input_function, context);
		validator->call_context = AllocSetContextCreate(context,
														"LagoDB text input validation",
														ALLOCSET_DEFAULT_SIZES);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	return validator;
}

bool
lagodb_text_input_accepts(LagodbTextInputValidator *validator,
						  const char *value)
{
	ErrorSaveContext escontext = {0};
	Datum		result;
	MemoryContext oldcontext;
	bool		accepted;

	MemoryContextReset(validator->call_context);
	oldcontext = MemoryContextSwitchTo(validator->call_context);
	escontext.type = T_ErrorSaveContext;
	PG_TRY();
	{
		/*
		 * Input functions receive mutable text, so isolate the parser's field
		 * buffer.
		 */
		accepted = InputFunctionCallSafe(&validator->input_function,
										 pstrdup(value), validator->typioparam, -1,
										 (Node *) &escontext, &result);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	return accepted;
}

void
lagodb_end_text_input_validator(LagodbTextInputValidator *validator)
{
	MemoryContextDelete(validator->context);
}

/*
 * Local state for the source-derived text/CSV serializer. It is allocated
 * and released by the row encoder, independently of PostgreSQL's COPY
 * executor state and its private layout.
 */
struct LagodbCopyRowEncoder
{
	StringInfo	fe_msgbuf;

	int			file_encoding;
	bool		need_transcoding;
	bool		encoding_embeds_ascii;

	Relation	rel;
	List	   *attnumlist;

	CopyFormatOptions opts;

	MemoryContext copycontext;

	FmgrInfo   *out_functions;
	MemoryContext rowcontext;
};

static void
lagodb_copy_send_data(LagodbCopyRowEncoder *state,
					  const void *data, int len)
{
	appendBinaryStringInfo(state->fe_msgbuf, data, len);
}

static void
lagodb_copy_send_string(LagodbCopyRowEncoder *state, const char *value)
{
	lagodb_copy_send_data(state, value, strlen(value));
}

static void
lagodb_copy_send_char(LagodbCopyRowEncoder *state, char value)
{
	appendStringInfoCharMacro(state->fe_msgbuf, value);
}

#define LAGODB_COPY_DUMP_SO_FAR() \
	do { \
		if (ptr > start) \
			lagodb_copy_send_data(state, start, ptr - start); \
	} while (0)

/* Source-derived from CopyAttributeOutText in the PG17.0-PG17.10 epoch. */
static void
lagodb_copy_attribute_out_text(LagodbCopyRowEncoder *state,
							   const char *string)
{
	const char *ptr;
	const char *start;
	char		c;
	char		delimc = state->opts.delim[0];

	if (state->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), state->file_encoding);
	else
		ptr = string;

	start = ptr;
	if (state->encoding_embeds_ascii)
	{
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						if (c == delimc)
							break;
						ptr++;
						continue;
				}
				LAGODB_COPY_DUMP_SO_FAR();
				lagodb_copy_send_char(state, '\\');
				lagodb_copy_send_char(state, c);
				start = ++ptr;
			}
			else if (c == '\\' || c == delimc)
			{
				LAGODB_COPY_DUMP_SO_FAR();
				lagodb_copy_send_char(state, '\\');
				start = ptr++;
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(state->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						if (c == delimc)
							break;
						ptr++;
						continue;
				}
				LAGODB_COPY_DUMP_SO_FAR();
				lagodb_copy_send_char(state, '\\');
				lagodb_copy_send_char(state, c);
				start = ++ptr;
			}
			else if (c == '\\' || c == delimc)
			{
				LAGODB_COPY_DUMP_SO_FAR();
				lagodb_copy_send_char(state, '\\');
				start = ptr++;
			}
			else
				ptr++;
		}
	}
	LAGODB_COPY_DUMP_SO_FAR();
}

/* Source-derived from CopyAttributeOutCSV in the PG17.0-PG17.10 epoch. */
static void
lagodb_copy_attribute_out_csv(LagodbCopyRowEncoder *state,
							  const char *string, bool use_quote)
{
	const char *ptr;
	const char *start;
	char		c;
	char		delimc = state->opts.delim[0];
	char		quotec = state->opts.quote[0];
	char		escapec = state->opts.escape[0];
	bool		single_attr = (list_length(state->attnumlist) == 1);

	if (!use_quote && strcmp(string, state->opts.null_print) == 0)
		use_quote = true;

	if (state->need_transcoding)
		ptr = pg_server_to_any(string, strlen(string), state->file_encoding);
	else
		ptr = string;

	if (!use_quote)
	{
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			const char *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && state->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(state->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (!use_quote)
	{
		lagodb_copy_send_string(state, ptr);
		return;
	}

	lagodb_copy_send_char(state, quotec);
	start = ptr;
	while ((c = *ptr) != '\0')
	{
		if (c == quotec || c == escapec)
		{
			LAGODB_COPY_DUMP_SO_FAR();
			lagodb_copy_send_char(state, escapec);
			start = ptr;
		}
		if (IS_HIGHBIT_SET(c) && state->encoding_embeds_ascii)
			ptr += pg_encoding_mblen(state->file_encoding, ptr);
		else
			ptr++;
	}
	LAGODB_COPY_DUMP_SO_FAR();
	lagodb_copy_send_char(state, quotec);
}

#undef LAGODB_COPY_DUMP_SO_FAR

static ParseState *
lagodb_copy_parser_state(void)
{
	ParseState *pstate = make_parsestate(NULL);

	/* ProcessCopyOptions uses this for parser diagnostics. */
	pstate->p_sourcetext = "";
	return pstate;
}

LagodbCopyRowEncoder *
lagodb_begin_copy_row_encoder(Relation rel,
							  List *options)
{
	LagodbCopyRowEncoder *copy_state;
	MemoryContext copycontext;
	MemoryContext oldcontext;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			num_phys_attrs = tupdesc->natts;

	/*
	 * The Rust owner can be dropped by a reset callback on its executor query
	 * context. PostgreSQL deletes child contexts before invoking that
	 * callback, so this explicitly-owned context must not be a child of the
	 * owner context. It is deleted by lagodb_end_copy_row_encoder on every
	 * normal and ERROR cleanup path.
	 */
	copycontext = AllocSetContextCreate(TopMemoryContext,
										"lagodb COPY row encoder", ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(copycontext);
	copy_state = (LagodbCopyRowEncoder *) palloc0(
												  sizeof(LagodbCopyRowEncoder));
	copy_state->copycontext = copycontext;

	PG_TRY();
	{
		ParseState *pstate = lagodb_copy_parser_state();
		ListCell   *cur;

		ProcessCopyOptions(pstate, &copy_state->opts, false, options);

		if (copy_state->opts.binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("binary COPY row encoding is not supported")));

		copy_state->rel = rel;
		copy_state->attnumlist = CopyGetAttnums(tupdesc, rel, NIL);
		copy_state->opts.force_quote_flags = (bool *) palloc0(
															  num_phys_attrs * sizeof(bool));
		if (copy_state->opts.force_quote_all)
			MemSet(copy_state->opts.force_quote_flags, true,
				   num_phys_attrs * sizeof(bool));
		else if (copy_state->opts.force_quote != NIL)
		{
			List	   *force_quote_attnums = CopyGetAttnums(
															 tupdesc, rel, copy_state->opts.force_quote);

			foreach(cur, force_quote_attnums)
			{
				int			attnum = lfirst_int(cur);

				copy_state->opts.force_quote_flags[attnum - 1] = true;
			}
		}

		copy_state->file_encoding = copy_state->opts.file_encoding < 0 ?
			pg_get_client_encoding() : copy_state->opts.file_encoding;
		copy_state->need_transcoding =
			copy_state->file_encoding != GetDatabaseEncoding() &&
			copy_state->file_encoding != PG_SQL_ASCII;
		copy_state->encoding_embeds_ascii =
			PG_ENCODING_IS_CLIENT_ONLY(copy_state->file_encoding);
		copy_state->opts.null_print_client = copy_state->opts.null_print;
		copy_state->fe_msgbuf = makeStringInfo();
		copy_state->out_functions = (FmgrInfo *) palloc(
														num_phys_attrs * sizeof(FmgrInfo));
		foreach(cur, copy_state->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			Oid			out_func_oid;
			bool		isvarlena;
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			getTypeOutputInfo(attr->atttypid, &out_func_oid, &isvarlena);
			fmgr_info(out_func_oid, &copy_state->out_functions[attnum - 1]);
		}
		copy_state->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
													   "COPY TO", ALLOCSET_DEFAULT_SIZES);
		if (copy_state->need_transcoding)
			copy_state->opts.null_print_client = pg_server_to_any(
																  copy_state->opts.null_print, copy_state->opts.null_print_len,
																  copy_state->file_encoding);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(copycontext);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	return copy_state;
}

void
lagodb_encode_copy_header(LagodbCopyRowEncoder *copy_state,
						  const char **data, int *len)
{
	TupleDesc	tupdesc = RelationGetDescr(copy_state->rel);
	ListCell   *cur;
	bool		need_delim = false;

	resetStringInfo(copy_state->fe_msgbuf);
	foreach(cur, copy_state->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		char	   *name = NameStr(TupleDescAttr(tupdesc, attnum - 1)->attname);

		if (need_delim)
			lagodb_copy_send_char(copy_state, copy_state->opts.delim[0]);
		need_delim = true;
		if (copy_state->opts.csv_mode)
			lagodb_copy_attribute_out_csv(copy_state, name, false);
		else
			lagodb_copy_attribute_out_text(copy_state, name);
	}
	*data = copy_state->fe_msgbuf->data;
	*len = copy_state->fe_msgbuf->len;
}

void
lagodb_encode_copy_row(LagodbCopyRowEncoder *copy_state, TupleTableSlot *slot,
					   const char **data, int *len)
{
	FmgrInfo   *out_functions = copy_state->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	bool		need_delim = false;
	char	   *string;

	resetStringInfo(copy_state->fe_msgbuf);
	MemoryContextReset(copy_state->rowcontext);
	oldcontext = MemoryContextSwitchTo(copy_state->rowcontext);
	PG_TRY();
	{
		/* TupleSlotRow has already populated the slot's Datum arrays. */
		foreach(cur, copy_state->attnumlist)
		{
			int			attnum = lfirst_int(cur);
			Datum		value = slot->tts_values[attnum - 1];
			bool		isnull = slot->tts_isnull[attnum - 1];

			if (need_delim)
				lagodb_copy_send_char(copy_state, copy_state->opts.delim[0]);
			need_delim = true;
			if (isnull)
				lagodb_copy_send_string(copy_state,
										copy_state->opts.null_print_client);
			else
			{
				string = OutputFunctionCall(&out_functions[attnum - 1], value);
				if (copy_state->opts.csv_mode)
					lagodb_copy_attribute_out_csv(copy_state, string,
												  copy_state->opts.force_quote_flags[attnum - 1]);
				else
					lagodb_copy_attribute_out_text(copy_state, string);
			}
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	*data = copy_state->fe_msgbuf->data;
	*len = copy_state->fe_msgbuf->len;
}

void
lagodb_end_copy_row_encoder(LagodbCopyRowEncoder *copy_state)
{
	MemoryContext copycontext;

	if (copy_state == NULL)
		return;
	copycontext = copy_state->copycontext;
	MemoryContextDelete(copycontext);
}

List *
lagodb_copy_get_attnums(Relation rel, List *attnamelist)
{
	return CopyGetAttnums(RelationGetDescr(rel), rel, attnamelist);
}
