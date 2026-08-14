#ifndef LAKEBASE_COPY_H
#define LAKEBASE_COPY_H

#include "lakebase_pg_compat.h"

#include "commands/copy.h"

/*
 * The preparation object mirrors the part of PostgreSQL's DoCopy contract
 * that must happen before BeginCopyFrom/BeginCopyTo.  The relation is kept
 * open when non-NULL and is closed by lakebase_dispose_copy_preparation().
 */
typedef struct LakebaseCopyPreparation
{
	Relation	relation;
	Node	   *where_clause;
	RawStmt    *raw_query;
	Oid			query_rel_id;
} LakebaseCopyPreparation;

void lakebase_prepare_copy_from(
	ParseState *pstate,
	const CopyStmt *stmt,
	int stmt_location,
	int stmt_len,
	LakebaseCopyPreparation *preparation);

void lakebase_prepare_copy_to(
	ParseState *pstate,
	const CopyStmt *stmt,
	int stmt_location,
	int stmt_len,
	LakebaseCopyPreparation *preparation);

void lakebase_dispose_copy_preparation(
	LakebaseCopyPreparation *preparation);

CopyFromState lakebase_begin_copy_from(
    ParseState *pstate,
    Relation rel,
    Node *where_clause,
    const char *filename,
    bool is_program,
    copy_data_source_cb data_source_cb,
    List *attnamelist,
    List *options);

bool lakebase_next_copy_from(
    CopyFromState state,
    ExprContext *econtext,
    Datum *values,
    bool *nulls);

uint64 lakebase_copy_from(CopyFromState state);
void lakebase_end_copy_from(CopyFromState state);

CopyToState lakebase_begin_copy_row_encoder(
    Relation rel,
    List *options);

void lakebase_encode_copy_header(
    CopyToState state,
    const char **data,
    int *len);
void lakebase_encode_copy_row(
    CopyToState state,
    TupleTableSlot *slot,
    const char **data,
    int *len);
void lakebase_end_copy_row_encoder(
    CopyToState state);

CopyToState lakebase_begin_copy_to(
    ParseState *pstate,
    Relation rel,
    RawStmt *raw_query,
    Oid query_rel_id,
    const char *filename,
    bool is_program,
    copy_data_dest_cb data_dest_cb,
    List *attnamelist,
    List *options);

uint64 lakebase_copy_to(CopyToState state);
void lakebase_end_copy_to(CopyToState state);
List *lakebase_copy_get_attnums(Relation rel, List *attnamelist);
TupleDesc lakebase_copy_to_tuple_desc(CopyToState state);
List *lakebase_copy_to_attnums(CopyToState state);

typedef struct LakebaseRawFieldReader LakebaseRawFieldReader;
typedef struct LakebaseTextInputValidator LakebaseTextInputValidator;

LakebaseRawFieldReader *lakebase_begin_raw_field_reader(
    copy_data_source_cb data_source_cb,
    List *options);
bool lakebase_next_raw_fields(
    LakebaseRawFieldReader *reader,
    char ***fields,
    size_t *field_count);
void lakebase_end_raw_field_reader(LakebaseRawFieldReader *reader);

LakebaseTextInputValidator *lakebase_begin_text_input_validator(Oid type_oid);
bool lakebase_text_input_accepts(
    LakebaseTextInputValidator *validator,
    const char *value);
void lakebase_end_text_input_validator(LakebaseTextInputValidator *validator);

#endif
