-- Run :fixture_command and stop the testcase on any nonzero exit.
-- PostgreSQL 15's psql has no SHELL_ERROR, so the status returns through a
-- backtick. The command's stdout goes to stderr, which pg_regress writes to
-- the same result file. Include with \i from the suite root: pg_regress feeds
-- top-level SQL via stdin. Only failed infrastructure commands enable
-- ON_ERROR_STOP; expected SQL errors in the actual regression cases retain
-- their normal pg_regress behavior.
\set fixture_failed `if :fixture_command >&2; then echo false; else echo true; fi`
\unset fixture_command
\if :fixture_failed
\set ON_ERROR_STOP on
DO $$ BEGIN RAISE EXCEPTION 'Regression fixture command failed'; END $$;
\endif
