#include "postgres.h"

#include "lakebase_copy.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "miscadmin.h"
#include "tcop/utility.h"

#if !LAKEBASE_PG17
#error "COPY bridge has not been ported to this PostgreSQL major version"
#endif

/*
 * The row encoder mirrors private copyto.c state from the audited PG17.0-
 * PG17.10 epoch.  Keep minor-version branches local to this code when a
 * future audit finds a relevant private-layout or serializer change.
 */

static void
lakebase_check_copy_utility(bool is_from)
{
	/* A consuming hook bypasses standard_ProcessUtility's recursion guard. */
	check_stack_depth();

	/*
	 * standard_ProcessUtility performs this check before DoCopy.  A consuming
	 * utility route does not pass through that generic dispatcher, so keep the
	 * same COPY FROM classification at the bridge boundary.  COPY FROM's
	 * read-only-transaction exception is checked later against the target
	 * relation, just as DoCopy does.  COPY TO is strictly read-only in PG17 and
	 * therefore does not enter this generic restriction block.
	 */
	if (is_from && (XactReadOnly || IsInParallelMode()))
	{
		PreventCommandIfParallelMode("COPY");
		PreventCommandDuringRecovery("COPY");
	}
}

static Node *
lakebase_prepare_where_clause(ParseState *pstate,
								  const CopyStmt *stmt,
							  Relation rel)
{
	Node	   *where_clause;
#if PG_VERSION_NUM >= 170007
	Bitmapset  *expr_attrs = NULL;
	int			i;
#endif

	if (stmt->whereClause == NULL)
		return NULL;

	/* Keep this sequence aligned with PostgreSQL's DoCopy preparation epoch. */
	where_clause = transformExpr(pstate, stmt->whereClause,
								 EXPR_KIND_COPY_WHERE);
	where_clause = coerce_to_boolean(pstate, where_clause, "WHERE");
	assign_expr_collations(pstate, where_clause);

#if PG_VERSION_NUM >= 170007
	/* PG17.7 introduced generated-column validation for COPY FROM WHERE. */
	pull_varattnos(where_clause, 1, &expr_attrs);
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, expr_attrs))
	{
		expr_attrs = bms_add_range(expr_attrs,
									1 - FirstLowInvalidHeapAttributeNumber,
									RelationGetNumberOfAttributes(rel) -
									FirstLowInvalidHeapAttributeNumber);
		expr_attrs = bms_del_member(expr_attrs,
									0 - FirstLowInvalidHeapAttributeNumber);
	}

	i = -1;
	while ((i = bms_next_member(expr_attrs, i)) >= 0)
	{
		AttrNumber attno = i + FirstLowInvalidHeapAttributeNumber;

		Assert(attno != 0);
		/* The attno guard is also required on PG17.7-17.9. */
		if (attno > 0 &&
			TupleDescAttr(RelationGetDescr(rel), attno - 1)->attgenerated)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("generated columns are not supported in COPY FROM WHERE conditions"),
					 errdetail("Column \"%s\" is a generated column.",
							   get_attname(RelationGetRelid(rel), attno, false))));
	}
#endif

	where_clause = eval_const_expressions(NULL, where_clause);
	where_clause = (Node *) canonicalize_qual((Expr *) where_clause, false);
	return (Node *) make_ands_implicit((Expr *) where_clause);
}

static Relation
lakebase_prepare_relation(ParseState *pstate,
						  const CopyStmt *stmt,
						  LOCKMODE lockmode,
						  Node **where_clause)
{
	ParseNamespaceItem *nsitem;
	RTEPermissionInfo *perminfo;
	List		   *attnums;
	ListCell	   *cur;
	Relation		rel;

	rel = table_openrv(stmt->relation, lockmode);
	nsitem = addRangeTableEntryForRelation(pstate, rel, lockmode,
									   NULL, false, false);
	perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = stmt->is_from ? ACL_INSERT : ACL_SELECT;

	if (stmt->whereClause != NULL)
	{
		/* COPY FROM WHERE names the target relation's columns. */
		addNSItemToQuery(pstate, nsitem, false, true, true);
		*where_clause = lakebase_prepare_where_clause(pstate, stmt, rel);
	}

	attnums = CopyGetAttnums(RelationGetDescr(rel), rel, stmt->attlist);
	foreach(cur, attnums)
	{
		int			attno = lfirst_int(cur);
		Bitmapset **columns = stmt->is_from ? &perminfo->insertedCols :
			&perminfo->selectedCols;

		*columns = bms_add_member(*columns,
								  attno - FirstLowInvalidHeapAttributeNumber);
	}
	ExecCheckPermissions(pstate->p_rtable,
						 list_make1(perminfo), true);
	return rel;
}

static RawStmt *
lakebase_relation_query(const CopyStmt *stmt, Relation rel,
							int stmt_location, int stmt_len)
{
	SelectStmt *select;
	ColumnRef  *cr;
	ResTarget  *target;
	RangeVar   *from;
	List		 *target_list = NIL;

	if (stmt->attlist == NIL)
	{
		cr = makeNode(ColumnRef);
		cr->fields = list_make1(makeNode(A_Star));
		cr->location = -1;

		target = makeNode(ResTarget);
		target->val = (Node *) cr;
		target->location = -1;
		target_list = list_make1(target);
	}
	else
	{
		ListCell *lc;

		foreach(lc, stmt->attlist)
		{
			cr = makeNode(ColumnRef);
			cr->fields = list_make1(lfirst(lc));
			cr->location = -1;

			target = makeNode(ResTarget);
			target->val = (Node *) cr;
			target->location = -1;
			target_list = lappend(target_list, target);
		}
	}

	from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
						pstrdup(RelationGetRelationName(rel)), -1);
	from->inh = false;

	select = makeNode(SelectStmt);
	select->targetList = target_list;
	select->fromClause = list_make1(from);

	RawStmt *query = makeNode(RawStmt);
	query->stmt = (Node *) select;
	query->stmt_location = stmt_location;
	query->stmt_len = stmt_len;
	return query;
}

static void
lakebase_close_preparation_relation(LakebaseCopyPreparation *preparation)
{
	if (preparation->relation != NULL)
	{
		table_close(preparation->relation, NoLock);
		preparation->relation = NULL;
	}
}

void
lakebase_prepare_copy_from(ParseState *pstate,
						   const CopyStmt *stmt,
						   int stmt_location,
						   int stmt_len,
						   LakebaseCopyPreparation *preparation)
{
	LakebaseCopyPreparation local = {0};

	(void) stmt_location;
	(void) stmt_len;
	Assert(stmt->relation != NULL);

	lakebase_check_copy_utility(stmt->is_from);

	PG_TRY();
	{
		local.relation = lakebase_prepare_relation(pstate, stmt,
										  RowExclusiveLock,
										  &local.where_clause);
		if (check_enable_rls(RelationGetRelid(local.relation),
								  InvalidOid, false) == RLS_ENABLED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY FROM not supported with row-level security"),
					 errhint("Use INSERT statements instead.")));
		if (XactReadOnly && !local.relation->rd_islocaltemp)
			PreventCommandIfReadOnly("COPY FROM");

		*preparation = local;
	}
	PG_CATCH();
	{
		lakebase_close_preparation_relation(&local);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
lakebase_prepare_copy_to(ParseState *pstate,
						 const CopyStmt *stmt,
						 int stmt_location,
						 int stmt_len,
						 LakebaseCopyPreparation *preparation)
{
	LakebaseCopyPreparation local = {0};

	lakebase_check_copy_utility(stmt->is_from);

	PG_TRY();
	{
		if (stmt->relation == NULL)
		{
			Assert(stmt->query != NULL);
			local.raw_query = makeNode(RawStmt);
			local.raw_query->stmt = stmt->query;
			local.raw_query->stmt_location = stmt_location;
			local.raw_query->stmt_len = stmt_len;
		}
		else
		{
			local.relation = lakebase_prepare_relation(pstate, stmt,
											AccessShareLock,
											NULL);
			local.query_rel_id = RelationGetRelid(local.relation);

			/*
			 * External COPY TO must be able to read a foreign table. PostgreSQL's
			 * server-file path rejects that relation kind, so normalize it to the
			 * same query form used for RLS before BeginCopyTo.
			 */
			if (check_enable_rls(local.query_rel_id, InvalidOid, false) == RLS_ENABLED ||
				local.relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			{
				local.raw_query = lakebase_relation_query(stmt,
												 local.relation,
												 stmt_location,
												 stmt_len);
				lakebase_close_preparation_relation(&local);
			}
		}

		*preparation = local;
	}
	PG_CATCH();
	{
		lakebase_close_preparation_relation(&local);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
lakebase_dispose_copy_preparation(LakebaseCopyPreparation *preparation)
{
	lakebase_close_preparation_relation(preparation);
	preparation->where_clause = NULL;
	preparation->raw_query = NULL;
	preparation->query_rel_id = InvalidOid;
}

CopyFromState
lakebase_begin_copy_from(ParseState *pstate,
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
lakebase_next_copy_from(CopyFromState state,
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

uint64
lakebase_copy_from(CopyFromState state)
{
	return CopyFrom(state);
}

void
lakebase_end_copy_from(CopyFromState state)
{
	EndCopyFrom(state);
}

/*
 * Exact layout of CopyToStateData in the audited PG17.0-PG17.10 copyto.c
 * epoch. The public API exposes CopyToState opaquely, but the row encoder must
 * initialize the same per-row state as DoCopyTo before using its source-derived
 * serializer.
 */
typedef enum LakebaseCopyDest
{
	LAKEBASE_COPY_FILE,
	LAKEBASE_COPY_FRONTEND,
	LAKEBASE_COPY_CALLBACK
} LakebaseCopyDest;

typedef struct LakebaseCopyToStateData
{
	LakebaseCopyDest copy_dest;
	FILE	   *copy_file;
	StringInfo	fe_msgbuf;

	int			file_encoding;
	bool		need_transcoding;
	bool		encoding_embeds_ascii;

	Relation	rel;
	QueryDesc  *queryDesc;
	List	   *attnumlist;
	char	   *filename;
	bool		is_program;
	copy_data_dest_cb data_dest_cb;

	CopyFormatOptions opts;
	Node	   *whereClause;

	MemoryContext copycontext;

	FmgrInfo   *out_functions;
	MemoryContext rowcontext;
	uint64		bytes_processed;
} LakebaseCopyToStateData;

static LakebaseCopyToStateData *
lakebase_copy_to_state(CopyToState state)
{
	return (LakebaseCopyToStateData *) state;
}

static void
lakebase_copy_send_data(LakebaseCopyToStateData *state,
						const void *data, int len)
{
	appendBinaryStringInfo(state->fe_msgbuf, data, len);
}

static void
lakebase_copy_send_string(LakebaseCopyToStateData *state, const char *value)
{
	lakebase_copy_send_data(state, value, strlen(value));
}

static void
lakebase_copy_send_char(LakebaseCopyToStateData *state, char value)
{
	appendStringInfoCharMacro(state->fe_msgbuf, value);
}

#define LAKEBASE_COPY_DUMP_SO_FAR() \
	do { \
		if (ptr > start) \
			lakebase_copy_send_data(state, start, ptr - start); \
	} while (0)

/* Source-derived from CopyAttributeOutText in the PG17.0-PG17.10 epoch. */
static void
lakebase_copy_attribute_out_text(LakebaseCopyToStateData *state,
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
					case '\b': c = 'b'; break;
					case '\f': c = 'f'; break;
					case '\n': c = 'n'; break;
					case '\r': c = 'r'; break;
					case '\t': c = 't'; break;
					case '\v': c = 'v'; break;
					default:
						if (c == delimc)
							break;
						ptr++;
						continue;
				}
				LAKEBASE_COPY_DUMP_SO_FAR();
				lakebase_copy_send_char(state, '\\');
				lakebase_copy_send_char(state, c);
				start = ++ptr;
			}
			else if (c == '\\' || c == delimc)
			{
				LAKEBASE_COPY_DUMP_SO_FAR();
				lakebase_copy_send_char(state, '\\');
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
					case '\b': c = 'b'; break;
					case '\f': c = 'f'; break;
					case '\n': c = 'n'; break;
					case '\r': c = 'r'; break;
					case '\t': c = 't'; break;
					case '\v': c = 'v'; break;
					default:
						if (c == delimc)
							break;
						ptr++;
						continue;
				}
				LAKEBASE_COPY_DUMP_SO_FAR();
				lakebase_copy_send_char(state, '\\');
				lakebase_copy_send_char(state, c);
				start = ++ptr;
			}
			else if (c == '\\' || c == delimc)
			{
				LAKEBASE_COPY_DUMP_SO_FAR();
				lakebase_copy_send_char(state, '\\');
				start = ptr++;
			}
			else
				ptr++;
		}
	}
	LAKEBASE_COPY_DUMP_SO_FAR();
}

/* Source-derived from CopyAttributeOutCSV in the PG17.0-PG17.10 epoch. */
static void
lakebase_copy_attribute_out_csv(LakebaseCopyToStateData *state,
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
		lakebase_copy_send_string(state, ptr);
		return;
	}

	lakebase_copy_send_char(state, quotec);
	start = ptr;
	while ((c = *ptr) != '\0')
	{
		if (c == quotec || c == escapec)
		{
			LAKEBASE_COPY_DUMP_SO_FAR();
			lakebase_copy_send_char(state, escapec);
			start = ptr;
		}
		if (IS_HIGHBIT_SET(c) && state->encoding_embeds_ascii)
			ptr += pg_encoding_mblen(state->file_encoding, ptr);
		else
			ptr++;
	}
	LAKEBASE_COPY_DUMP_SO_FAR();
	lakebase_copy_send_char(state, quotec);
}

#undef LAKEBASE_COPY_DUMP_SO_FAR

static ParseState *
lakebase_copy_parser_state(void)
{
	ParseState *pstate = make_parsestate(NULL);

	/* BeginCopyTo uses this for parser diagnostics and query planning. */
	pstate->p_sourcetext = "";
	return pstate;
}

static List *
lakebase_copy_shape_attnames(Relation rel)
{
	TupleDesc	tupDesc = RelationGetDescr(rel);
	List	   *attnamelist = NIL;
	int			i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, i);

		if (!attr->attisdropped)
			attnamelist = lappend(
				attnamelist,
				makeString(pstrdup(NameStr(attr->attname))));
	}
	return attnamelist;
}

static RawStmt *
lakebase_copy_shape_query(Relation rel)
{
	TupleDesc	tupDesc = RelationGetDescr(rel);
	SelectStmt *select = makeNode(SelectStmt);
	RawStmt    *raw_query = makeNode(RawStmt);
	int			i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, i);
		ResTarget  *target = makeNode(ResTarget);
		char		*name;

		if (attr->attisdropped)
		{
			/* Preserve physical attno positions; CopyGetAttnums skips this. */
			name = psprintf("__lakebase_dropped_%d", i + 1);
			target->val = (Node *) makeNullConst(INT4OID, -1, InvalidOid);
		}
		else
		{
			name = pstrdup(NameStr(attr->attname));
			target->val = (Node *) makeNullConst(attr->atttypid,
													 attr->atttypmod,
													 attr->attcollation);
		}
		target->name = name;
		select->targetList = lappend(select->targetList, target);
	}

	/* Header generation and encoder initialization must not read the table. */
	select->whereClause = (Node *) makeBoolConst(false, false);
	raw_query->stmt = (Node *) select;
	raw_query->stmt_location = -1;
	raw_query->stmt_len = 0;
	return raw_query;
}

static CopyToState
lakebase_begin_copy_shape(Relation rel,
							 List *options)
{
	ParseState *pstate = lakebase_copy_parser_state();
	RawStmt    *raw_query = lakebase_copy_shape_query(rel);
	List	   *attnamelist = lakebase_copy_shape_attnames(rel);

	return BeginCopyTo(pstate,
						 NULL,
						 raw_query,
						 InvalidOid,
						 NULL,
						 false,
						 NULL,
						 attnamelist,
						 options);
}

CopyToState
lakebase_begin_copy_row_encoder(Relation rel,
								List *options)
{
	CopyToState state = lakebase_begin_copy_shape(rel, options);

	PG_TRY();
	{
		LakebaseCopyToStateData *copy_state = lakebase_copy_to_state(state);
		TupleDesc	tupdesc = copy_state->queryDesc->tupDesc;
		ListCell   *cur;
		int			num_phys_attrs = tupdesc->natts;

		if (copy_state->opts.binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("binary COPY row encoding is not supported")));

		copy_state->opts.null_print_client = copy_state->opts.null_print;
		copy_state->fe_msgbuf = makeStringInfo();
		copy_state->out_functions = (FmgrInfo *) palloc(
			num_phys_attrs * sizeof(FmgrInfo));
		foreach(cur, copy_state->attnumlist)
		{
			int		attnum = lfirst_int(cur);
			Oid		out_func_oid;
			bool	isvarlena;
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
		lakebase_end_copy_row_encoder(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return state;
}

void
lakebase_encode_copy_header(CopyToState state, const char **data, int *len)
{
	LakebaseCopyToStateData *copy_state = lakebase_copy_to_state(state);
	TupleDesc	tupdesc = copy_state->queryDesc->tupDesc;
	ListCell   *cur;
	bool		need_delim = false;

	resetStringInfo(copy_state->fe_msgbuf);
	foreach(cur, copy_state->attnumlist)
	{
		int		attnum = lfirst_int(cur);
		char	   *name = NameStr(TupleDescAttr(tupdesc, attnum - 1)->attname);

		if (need_delim)
			lakebase_copy_send_char(copy_state, copy_state->opts.delim[0]);
		need_delim = true;
		if (copy_state->opts.csv_mode)
			lakebase_copy_attribute_out_csv(copy_state, name, false);
		else
			lakebase_copy_attribute_out_text(copy_state, name);
	}
	*data = copy_state->fe_msgbuf->data;
	*len = copy_state->fe_msgbuf->len;
}

void
lakebase_encode_copy_row(CopyToState state, TupleTableSlot *slot,
						 const char **data, int *len)
{
	LakebaseCopyToStateData *copy_state = lakebase_copy_to_state(state);
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
		slot_getallattrs(slot);
		foreach(cur, copy_state->attnumlist)
		{
			int		attnum = lfirst_int(cur);
			Datum	value = slot->tts_values[attnum - 1];
			bool	isnull = slot->tts_isnull[attnum - 1];

			if (need_delim)
				lakebase_copy_send_char(copy_state, copy_state->opts.delim[0]);
			need_delim = true;
			if (isnull)
				lakebase_copy_send_string(copy_state,
					copy_state->opts.null_print_client);
			else
			{
				string = OutputFunctionCall(&out_functions[attnum - 1], value);
				if (copy_state->opts.csv_mode)
					lakebase_copy_attribute_out_csv(copy_state, string,
						copy_state->opts.force_quote_flags[attnum - 1]);
				else
					lakebase_copy_attribute_out_text(copy_state, string);
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
lakebase_end_copy_row_encoder(CopyToState state)
{
	if (state == NULL)
		return;
	if (lakebase_copy_to_state(state)->rowcontext != NULL)
		MemoryContextDelete(lakebase_copy_to_state(state)->rowcontext);
	EndCopyTo(state);
}

CopyToState
lakebase_begin_copy_to(ParseState *pstate,
					   Relation rel,
					   RawStmt *raw_query,
					   Oid query_rel_id,
					   const char *filename,
					   bool is_program,
					   copy_data_dest_cb data_dest_cb,
					   List *attnamelist,
					   List *options)
{
	return BeginCopyTo(pstate,
						rel,
						raw_query,
						query_rel_id,
						filename,
						is_program,
						data_dest_cb,
						attnamelist,
						options);
}

uint64
lakebase_copy_to(CopyToState state)
{
	return DoCopyTo(state);
}

void
lakebase_end_copy_to(CopyToState state)
{
	EndCopyTo(state);
}

List *
lakebase_copy_get_attnums(Relation rel, List *attnamelist)
{
	return CopyGetAttnums(RelationGetDescr(rel), rel, attnamelist);
}

TupleDesc
lakebase_copy_to_tuple_desc(CopyToState state)
{
	LakebaseCopyToStateData *copy = lakebase_copy_to_state(state);

	Assert(copy != NULL);
	if (copy->rel != NULL)
		return RelationGetDescr(copy->rel);
	Assert(copy->queryDesc != NULL);
	return copy->queryDesc->tupDesc;
}

List *
lakebase_copy_to_attnums(CopyToState state)
{
	LakebaseCopyToStateData *copy = lakebase_copy_to_state(state);

	Assert(copy != NULL);
	return copy->attnumlist;
}
