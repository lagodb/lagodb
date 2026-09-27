/*
 * Command preparation derived from PostgreSQL's DoCopy.
 *
 * This module owns the endpoint, utility, permission, RLS, and expression
 * contracts that precede the byte and typed COPY executors. Executor-specific
 * workspaces remain in lagodb_copy.c and the COPY FROM/TO forks.
 */
#include "postgres.h"

#include "lagodb_copy.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_authid_d.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/miscnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"

#if !LAGODB_PG17
#error "COPY preparation has not been ported to this PostgreSQL major version"
#endif

static void
lagodb_prepare_copy_command(const CopyStmt *stmt, LagodbCopyEndpoint endpoint)
{
	/* A consuming hook bypasses standard_ProcessUtility's recursion guard. */
	check_stack_depth();

	/*
	 * standard_ProcessUtility performs this check before DoCopy.  A consuming
	 * utility route does not pass through that generic dispatcher, so keep
	 * the same COPY FROM classification at the bridge boundary.  COPY FROM's
	 * read-only-transaction exception is checked later against the target
	 * relation, just as DoCopy does.  COPY TO is strictly read-only in
	 * PostgreSQL and therefore does not enter this generic restriction block.
	 */
	if (stmt->is_from && (XactReadOnly || IsInParallelMode()))
	{
		PreventCommandIfParallelMode("COPY");
		PreventCommandDuringRecovery("COPY");
	}

	/*
	 * DoCopy checks endpoint privileges before opening the relation or any
	 * I/O. External URI consumers supply their own callbacks and never open
	 * the URI as a server filename. PROGRAM classification takes precedence
	 * over URI recognition, so a URI in a shell command cannot bypass this
	 * check. Role checks and diagnostics below follow PostgreSQL 17.10
	 * copy.c.
	 */
	if (endpoint == LAGODB_COPY_SERVER_PROGRAM)
	{
		if (!has_privs_of_role(GetUserId(), ROLE_PG_EXECUTE_SERVER_PROGRAM))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to COPY to or from an external program"),
					 errdetail("Only roles with privileges of the \"%s\" role may COPY to or from an external program.",
							   "pg_execute_server_program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
	}
	else if (endpoint == LAGODB_COPY_SERVER_FILE)
	{
		if (stmt->is_from && !has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to COPY from a file"),
					 errdetail("Only roles with privileges of the \"%s\" role may COPY from a file.",
							   "pg_read_server_files"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));

		if (!stmt->is_from && !has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to COPY to a file"),
					 errdetail("Only roles with privileges of the \"%s\" role may COPY to a file.",
							   "pg_write_server_files"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
	}
}

static Node *
lagodb_prepare_where_clause(ParseState *pstate,
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
		AttrNumber	attno = i + FirstLowInvalidHeapAttributeNumber;

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
lagodb_prepare_relation(ParseState *pstate,
						const CopyStmt *stmt,
						LOCKMODE lockmode,
						Node **where_clause)
{
	ParseNamespaceItem *nsitem;
	RTEPermissionInfo *perminfo;
	List	   *attnums;
	ListCell   *cur;
	Relation	rel;

	rel = table_openrv(stmt->relation, lockmode);
	nsitem = addRangeTableEntryForRelation(pstate, rel, lockmode,
										   NULL, false, false);
	perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = stmt->is_from ? ACL_INSERT : ACL_SELECT;

	if (stmt->whereClause != NULL)
	{
		/* COPY FROM WHERE names the target relation's columns. */
		addNSItemToQuery(pstate, nsitem, false, true, true);
		*where_clause = lagodb_prepare_where_clause(pstate, stmt, rel);
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
lagodb_relation_query(const CopyStmt *stmt, Relation rel,
					  int stmt_location, int stmt_len)
{
	SelectStmt *select;
	ColumnRef  *cr;
	ResTarget  *target;
	RangeVar   *from;
	List	   *target_list = NIL;

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
		ListCell   *lc;

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

	RawStmt    *query = makeNode(RawStmt);

	query->stmt = (Node *) select;
	query->stmt_location = stmt_location;
	query->stmt_len = stmt_len;
	return query;
}

static void
lagodb_close_preparation_relation(LagodbCopyPreparation *preparation)
{
	if (preparation->relation != NULL)
	{
		table_close(preparation->relation, NoLock);
		preparation->relation = NULL;
	}
}

void
lagodb_prepare_copy_from(ParseState *pstate,
						 const CopyStmt *stmt,
						 LagodbCopyEndpoint endpoint,
						 int stmt_location,
						 int stmt_len,
						 LagodbCopyPreparation *preparation)
{
	LagodbCopyPreparation local = {0};

	(void) stmt_location;
	(void) stmt_len;
	Assert(stmt->relation != NULL);

	lagodb_prepare_copy_command(stmt, endpoint);

	PG_TRY();
	{
		local.relation = lagodb_prepare_relation(pstate, stmt,
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
		lagodb_close_preparation_relation(&local);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
lagodb_prepare_copy_to(ParseState *pstate,
					   const CopyStmt *stmt,
					   LagodbCopyEndpoint endpoint,
					   int stmt_location,
					   int stmt_len,
					   LagodbCopyPreparation *preparation)
{
	LagodbCopyPreparation local = {0};

	lagodb_prepare_copy_command(stmt, endpoint);

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
			local.relation = lagodb_prepare_relation(pstate, stmt,
													 AccessShareLock,
													 NULL);
			local.query_rel_id = RelationGetRelid(local.relation);

			/*
			 * External COPY TO must be able to read a foreign table.
			 * PostgreSQL's server-file path rejects that relation kind, so
			 * normalize it to the same query form used for RLS before
			 * BeginCopyTo.
			 */
			if (check_enable_rls(local.query_rel_id, InvalidOid, false) == RLS_ENABLED ||
				local.relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			{
				local.raw_query = lagodb_relation_query(stmt,
														local.relation,
														stmt_location,
														stmt_len);
				lagodb_close_preparation_relation(&local);
			}
		}

		*preparation = local;
	}
	PG_CATCH();
	{
		lagodb_close_preparation_relation(&local);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
lagodb_dispose_copy_preparation(LagodbCopyPreparation *preparation)
{
	lagodb_close_preparation_relation(preparation);
	preparation->where_clause = NULL;
	preparation->raw_query = NULL;
	preparation->query_rel_id = InvalidOid;
}
