#include "lakebase_injection_point.h"

void
lakebase_injection_point_run(const char *name)
{
    LAKEBASE_INJECTION_POINT(name);
}
