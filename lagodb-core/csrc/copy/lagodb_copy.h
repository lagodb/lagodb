#ifndef LAGODB_COPY_H
#define LAGODB_COPY_H

#include "lagodb_pg_compat.h"

#include "commands/copy.h"
#include "lagodb_relation.h"

/* Exact-build counterpart of Rust's CopyEndpoint. */
typedef enum LagodbCopyEndpoint
{
	LAGODB_COPY_CLIENT_STREAM = 0,
	LAGODB_COPY_SERVER_FILE = 1,
	LAGODB_COPY_SERVER_PROGRAM = 2,
	LAGODB_COPY_EXTERNAL_URI = 3
} LagodbCopyEndpoint;

/*
 * The preparation object mirrors the part of PostgreSQL's DoCopy contract
 * that must happen before BeginCopyFrom/BeginCopyTo.  The relation is kept
 * open when non-NULL and is closed by lagodb_dispose_copy_preparation().
 */
typedef struct LagodbCopyPreparation
{
	Relation	relation;
	Node	   *where_clause;
	RawStmt    *raw_query;
	Oid			query_rel_id;
} LagodbCopyPreparation;

/*
 * Typed COPY callbacks are intentionally Datum/slot based and contain no
 * provider-specific types.  Each callback receives opaque Rust-owned state;
 * PostgreSQL invokes these functions synchronously and never retains or uses
 * that state outside the routed execute/end lifetime.
 */
typedef enum LagodbTypedCopyRowResult
{
	LAGODB_TYPED_COPY_ROW = 0,
	LAGODB_TYPED_COPY_END,
	LAGODB_TYPED_COPY_REJECTED
} LagodbTypedCopyRowResult;

typedef LagodbTypedCopyRowResult (*lagodb_typed_copy_source_cb) (
																 void *context,
																 Datum *values,
																 bool *nulls,
																 uint64 *bytes_consumed,
																 uint64 *materialized_bytes,
																 const char **rejection_message,
																 const char **rejection_location,
																 int *rejection_column,
																 int *rejection_sql_error_code);

typedef void (*lagodb_typed_copy_dest_cb) (void *context,
										   TupleTableSlot *slot,
										   uint64 *bytes_produced);

void		lagodb_prepare_copy_from(
									 ParseState *pstate,
									 const CopyStmt *stmt,
									 LagodbCopyEndpoint endpoint,
									 int stmt_location,
									 int stmt_len,
									 LagodbCopyPreparation *preparation);

void		lagodb_prepare_copy_to(
								   ParseState *pstate,
								   const CopyStmt *stmt,
								   LagodbCopyEndpoint endpoint,
								   int stmt_location,
								   int stmt_len,
								   LagodbCopyPreparation *preparation);

void		lagodb_dispose_copy_preparation(
											LagodbCopyPreparation *preparation);

CopyFromState lagodb_begin_copy_from(
									 ParseState *pstate,
									 Relation rel,
									 Node *where_clause,
									 const char *filename,
									 bool is_program,
									 copy_data_source_cb data_source_cb,
									 List *attnamelist,
									 List *options);

bool		lagodb_next_copy_from(
								  CopyFromState state,
								  ExprContext *econtext,
								  Datum *values,
								  bool *nulls);

void		lagodb_end_copy_from(CopyFromState state);

CopyFromState lagodb_begin_routed_copy_from(
											ParseState *pstate,
											Relation rel,
											Node *where_clause,
											const char *filename,
											bool is_program,
											copy_data_source_cb data_source_cb,
											List *attnamelist,
											List *options,
											bool typed_input);
uint64		lagodb_execute_routed_copy_from(
											CopyFromState state,
											lagodb_typed_copy_source_cb typed_source,
											void *typed_source_context,
											bool provider_owned_partitioned_table);
void		lagodb_end_routed_copy_from(CopyFromState state);
TupleDesc	lagodb_routed_copy_from_tuple_desc(CopyFromState state);
List	   *lagodb_routed_copy_from_attnums(CopyFromState state);

typedef struct LagodbCopyRowEncoder LagodbCopyRowEncoder;

LagodbCopyRowEncoder *lagodb_begin_copy_row_encoder(
													Relation rel,
													List *options);

void		lagodb_encode_copy_header(
									  LagodbCopyRowEncoder *state,
									  const char **data,
									  int *len);
void		lagodb_encode_copy_row(
								   LagodbCopyRowEncoder *state,
								   TupleTableSlot *slot,
								   const char **data,
								   int *len);
void		lagodb_end_copy_row_encoder(
										LagodbCopyRowEncoder *state);

CopyToState lagodb_begin_routed_copy_to(
										ParseState *pstate,
										Relation rel,
										RawStmt *raw_query,
										Oid query_rel_id,
										const char *filename,
										bool is_program,
										copy_data_dest_cb data_dest_cb,
										List *attnamelist,
										List *options,
										bool provider_owned_partitioned_table,
										bool typed_output);
uint64		lagodb_execute_routed_copy_to(
										  CopyToState state,
										  lagodb_typed_copy_dest_cb typed_destination,
										  void *typed_destination_context);
void		lagodb_end_routed_copy_to(CopyToState state, bool is_error);
void		lagodb_finish_routed_copy_to(CopyToState state);
void		lagodb_update_routed_copy_to_progress(CopyToState state,
												  uint64 bytes_produced);
TupleDesc	lagodb_routed_copy_to_tuple_desc(CopyToState state);
List	   *lagodb_routed_copy_to_attnums(CopyToState state);
List	   *lagodb_copy_get_attnums(Relation rel, List *attnamelist);

typedef struct LagodbRawFieldReader LagodbRawFieldReader;
typedef struct LagodbTextInputValidator LagodbTextInputValidator;
typedef struct LagodbCopyDatumCoercion LagodbCopyDatumCoercion;

LagodbCopyDatumCoercion *lagodb_begin_copy_datum_coercion(
														  Oid type_oid, int32 source_typmod, int32 target_typmod);
Datum		lagodb_coerce_copy_datum(LagodbCopyDatumCoercion *coercion, Datum value);
void		lagodb_end_copy_datum_coercion(LagodbCopyDatumCoercion *coercion);

LagodbRawFieldReader *lagodb_begin_raw_field_reader(
													copy_data_source_cb data_source_cb,
													List *options);
bool		lagodb_next_raw_fields(
								   LagodbRawFieldReader *reader,
								   char ***fields,
								   size_t *field_count);
void		lagodb_end_raw_field_reader(LagodbRawFieldReader *reader);

LagodbTextInputValidator *lagodb_begin_text_input_validator(Oid type_oid);
bool		lagodb_text_input_accepts(
									  LagodbTextInputValidator *validator,
									  const char *value);
void		lagodb_end_text_input_validator(LagodbTextInputValidator *validator);

#endif
