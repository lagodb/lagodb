/* Target typmod assignment for externally produced COPY datums. */
#include "postgres.h"

#include "lagodb_copy.h"

#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

struct LagodbCopyDatumCoercion
{
	MemoryContext context;
	ExprState  *expression;
	ExprContext econtext;
};

LagodbCopyDatumCoercion *
lagodb_begin_copy_datum_coercion(Oid type_oid, int32 source_typmod,
								 int32 target_typmod)
{
	LagodbCopyDatumCoercion *coercion;
	MemoryContext context;
	MemoryContext oldcontext;
	Oid			funcid;

	/* Unconstrained columns have no expression or per-value PG call. */
	if (target_typmod < 0 || target_typmod == source_typmod ||
		find_typmod_coercion_function(type_oid, &funcid) == COERCION_PATH_NONE)
		return NULL;

	context = AllocSetContextCreate(CurrentMemoryContext,
									"COPY datum coercion",
									ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(context);
	PG_TRY();
	{
		CaseTestExpr *input = makeNode(CaseTestExpr);
		Node	   *expression;

		input->typeId = type_oid;
		input->typeMod = source_typmod;
		input->collation = get_typcollation(type_oid);

		/*
		 * The parser owns scalar and array typmod rules, including the
		 * isExplicit=false argument used by assignment coercions.
		 */
		expression = coerce_to_target_type(NULL, (Node *) input, type_oid,
										   type_oid, target_typmod, COERCION_ASSIGNMENT,
										   COERCE_IMPLICIT_CAST, -1);
		assign_expr_collations(NULL, expression);
		expression = (Node *) expression_planner((Expr *) expression);

		/*
		 * The planner copies CaseTestExpr nodes. Use PG's structural equality
		 * to recognize the placeholder, including temporal support functions'
		 * no-op relabels, rather than comparing pre/post-planning addresses.
		 */
		if (equal(expression, input) ||
			(IsA(expression, RelabelType) &&
			 equal(((RelabelType *) expression)->arg, input)))
			coercion = NULL;
		else
		{
			coercion = palloc0(sizeof(LagodbCopyDatumCoercion));
			coercion->context = context;
			coercion->econtext.ecxt_per_query_memory = context;
			coercion->expression = ExecInitExpr((Expr *) expression, NULL);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(oldcontext);
	if (coercion == NULL)
		MemoryContextDelete(context);
	return coercion;
}

Datum
lagodb_coerce_copy_datum(LagodbCopyDatumCoercion *coercion, Datum value)
{
	bool		isnull;
	Datum		result;

	/*
	 * CaseTestExpr reads this externally supplied value, as in PG's array and
	 * domain coercion expressions. Results belong to the COPY row.
	 */
	coercion->econtext.caseValue_datum = value;
	coercion->econtext.ecxt_per_tuple_memory = CurrentMemoryContext;
	result = ExecEvalExpr(coercion->expression, &coercion->econtext, &isnull);
	Assert(!isnull);
	return result;
}

void
lagodb_end_copy_datum_coercion(LagodbCopyDatumCoercion *coercion)
{
	MemoryContextDelete(coercion->context);
}
