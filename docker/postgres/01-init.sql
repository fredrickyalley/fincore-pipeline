-- Runs once, on the first start of the postgres container.
-- POSTGRES_DB=fincore (the local warehouse, Redshift stand-in) already exists; add the
-- Airflow metadata database next to it and create the raw landing tables.
CREATE DATABASE airflow;

\connect fincore
\i /sql/postgres_ddl.sql
