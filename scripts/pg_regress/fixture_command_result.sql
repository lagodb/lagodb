-- psql's \! records an ordinary nonzero exit in SHELL_ERROR and keeps running.
-- Include with \i from the suite root: pg_regress feeds top-level SQL via stdin.
-- Only failed infrastructure commands enable ON_ERROR_STOP; expected SQL errors
-- in the actual regression cases retain their normal pg_regress behavior.
\if :SHELL_ERROR
\set ON_ERROR_STOP on
DO $$ BEGIN RAISE EXCEPTION 'Regression fixture command failed'; END $$;
\endif
