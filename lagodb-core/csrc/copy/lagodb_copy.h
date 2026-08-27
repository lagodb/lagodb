#ifndef LAGODB_COPY_H
#define LAGODB_COPY_H

#include "lagodb_pg_compat.h"

#include "commands/copy.h"

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

void lagodb_prepare_copy_from(
	ParseState *pstate,
	const CopyStmt *stmt,
	int stmt_location,
	int stmt_len,
	LagodbCopyPreparation *preparation);

void lagodb_prepare_copy_to(
	ParseState *pstate,
	const CopyStmt *stmt,
	int stmt_location,
	int stmt_len,
	LagodbCopyPreparation *preparation);

void lagodb_dispose_copy_preparation(
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

bool lagodb_next_copy_from(
    CopyFromState state,
    ExprContext *econtext,
    Datum *values,
    bool *nulls);

uint64 lagodb_copy_from(CopyFromState state);
void lagodb_end_copy_from(CopyFromState state);

CopyToState lagodb_begin_copy_row_encoder(
    Relation rel,
    List *options);

void lagodb_encode_copy_header(
    CopyToState state,
    const char **data,
    int *len);
void lagodb_encode_copy_row(
    CopyToState state,
    TupleTableSlot *slot,
    const char **data,
    int *len);
void lagodb_end_copy_row_encoder(
    CopyToState state);

CopyToState lagodb_begin_copy_to(
    ParseState *pstate,
    Relation rel,
    RawStmt *raw_query,
    Oid query_rel_id,
    const char *filename,
    bool is_program,
    copy_data_dest_cb data_dest_cb,
    List *attnamelist,
    List *options);

uint64 lagodb_copy_to(CopyToState state);
void lagodb_end_copy_to(CopyToState state);
List *lagodb_copy_get_attnums(Relation rel, List *attnamelist);
TupleDesc lagodb_copy_to_tuple_desc(CopyToState state);
List *lagodb_copy_to_attnums(CopyToState state);

typedef struct LagodbRawFieldReader LagodbRawFieldReader;
typedef struct LagodbTextInputValidator LagodbTextInputValidator;

LagodbRawFieldReader *lagodb_begin_raw_field_reader(
    copy_data_source_cb data_source_cb,
    List *options);
bool lagodb_next_raw_fields(
    LagodbRawFieldReader *reader,
    char ***fields,
    size_t *field_count);
void lagodb_end_raw_field_reader(LagodbRawFieldReader *reader);

LagodbTextInputValidator *lagodb_begin_text_input_validator(Oid type_oid);
bool lagodb_text_input_accepts(
    LagodbTextInputValidator *validator,
    const char *value);
void lagodb_end_text_input_validator(LagodbTextInputValidator *validator);

#endif
