#include "postgres.h"

#include "lakebase_copy.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "miscadmin.h"
#include "tcop/utility.h"

#if !LAKEBASE_PG17
#error "COPY bridge has not been ported to this PostgreSQL major version"
#endif

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

/* PG17-private CopyToStateData prefix, reviewed with this major's copyto.c. */
typedef struct LakebaseCopyToStatePrefix
{
	/* CopyDest is private to copyto.c; PG17 declares it as a C enum. */
	int			copy_dest;
	FILE	   *copy_file;
	StringInfo	fe_msgbuf;
	int			file_encoding;
	bool		need_transcoding;
	bool		encoding_embeds_ascii;
	Relation	rel;
	QueryDesc  *queryDesc;
	List	   *attnumlist;
} LakebaseCopyToStatePrefix;

TupleDesc
lakebase_copy_to_tuple_desc(CopyToState state)
{
	LakebaseCopyToStatePrefix *copy = (LakebaseCopyToStatePrefix *) state;

	Assert(copy != NULL);
	if (copy->rel != NULL)
		return RelationGetDescr(copy->rel);
	Assert(copy->queryDesc != NULL);
	return copy->queryDesc->tupDesc;
}

List *
lakebase_copy_to_attnums(CopyToState state)
{
	LakebaseCopyToStatePrefix *copy = (LakebaseCopyToStatePrefix *) state;

	Assert(copy != NULL);
	return copy->attnumlist;
}
