#ifndef LAGODB_INDEX_AM_H
#define LAGODB_INDEX_AM_H

#include "postgres.h"
#include "access/genam.h"

/*
 * Index access method bridge for operations PostgreSQL exposes only as
 * macros or through report fields an error value cannot carry. Declared in
 * Rust by src/index_am/scan/context.rs and src/index_am/insert/unique.rs.
 */
extern void lagodb_index_scan_record_search(IndexScanDesc scan);
extern void lagodb_index_unique_violation(Relation index, Relation heap,
										  const Datum *values,
										  const bool *isnull);

#endif
