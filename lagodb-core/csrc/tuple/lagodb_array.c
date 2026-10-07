#include "postgres.h"

#include "utils/arrayaccess.h"

#include "lagodb_array.h"

int
lagodb_array_cursor_setup(ArrayType *array, LagodbArrayCursor *cursor)
{
	array_iter	iter;
	int			nitems;

	nitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	array_iter_setup(&iter, (AnyArrayType *) array);
	cursor->data = iter.dataptr;
	cursor->bitmap = iter.bitmapptr;
	cursor->bitmask = iter.bitmask;
	return nitems;
}

Datum
lagodb_array_cursor_next(LagodbArrayCursor *cursor, int len,
						bool by_value, char align, bool *is_null)
{
	array_iter	iter;
	Datum		value;

	/*
	 * DetoastedVarlena has already flattened the array. Only the flat-array
	 * position crosses the ABI; PG owns datum fetching, alignment and bitmap
	 * advancement. The index argument is used only for expanded arrays.
	 */
	iter.datumptr = NULL;
	iter.isnullptr = NULL;
	iter.dataptr = cursor->data;
	iter.bitmapptr = cursor->bitmap;
	iter.bitmask = cursor->bitmask;
	value = array_iter_next(&iter, is_null, 0, len, by_value, align);
	cursor->data = iter.dataptr;
	cursor->bitmap = iter.bitmapptr;
	cursor->bitmask = iter.bitmask;
	return value;
}
