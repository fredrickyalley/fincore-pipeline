"""Warehouse load: DELETE the date, then load it, in one transaction per dataset.

aws:   Redshift COPY straight from S3 Parquet.
local: Postgres has no COPY FROM S3, so the Parquet files are read and bulk-inserted.
"""

import io
import logging
import re
from collections.abc import Sequence

from fincore_pipeline.redshift_sql import (
    LOAD_TARGETS,
    LoadTarget,
    build_copy_statements,
    build_delete_statement,
    build_delete_statements,
)
from fincore_pipeline.settings import (
    AWS_CONN_ID,
    IS_LOCAL,
    PROCESSED_BUCKET,
    REDSHIFT_COPY_ROLE_VARIABLE,
    WAREHOUSE_CONN_ID,
)

log = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
INSERT_PAGE_SIZE = 5000


def build_insert_statement(table: str, columns: Sequence[str]) -> str:
    bad = [column for column in columns if not _IDENTIFIER.fullmatch(column)]
    if bad:
        raise ValueError(f"Refusing unsafe column names: {bad}")
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"  # noqa: S608 - validated above


def _hook():  # type: ignore[no-untyped-def]
    from airflow.hooks.base import BaseHook

    return BaseHook.get_connection(WAREHOUSE_CONN_ID).get_hook()


def delete_partitions(trade_date: str) -> None:
    _hook().run(build_delete_statements(trade_date), autocommit=False)


def load_dataset(dataset: str, trade_date: str) -> None:
    target = LOAD_TARGETS[dataset]
    if IS_LOCAL:
        _insert_from_parquet(target, trade_date)
        return
    from airflow.models import Variable

    role_arn = Variable.get(REDSHIFT_COPY_ROLE_VARIABLE)
    _hook().run(build_copy_statements(target, trade_date, PROCESSED_BUCKET, role_arn), autocommit=False)


def _read_partition(target: LoadTarget, trade_date: str):  # type: ignore[no-untyped-def]
    import pyarrow as pa
    import pyarrow.parquet as pq
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    prefix = target.partition_prefix(trade_date)
    keys = [key for key in s3.list_keys(PROCESSED_BUCKET, prefix=prefix) or [] if key.endswith(".parquet")]
    tables = [
        pq.read_table(io.BytesIO(s3.get_key(key, PROCESSED_BUCKET).get()["Body"].read())) for key in keys
    ]
    return pa.concat_tables(tables) if tables else None


def _insert_from_parquet(target: LoadTarget, trade_date: str) -> None:
    from psycopg2.extras import execute_values

    table = _read_partition(target, trade_date)
    connection = _hook().get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(build_delete_statement(target, trade_date))
            if table is not None and table.num_rows:
                rows = [tuple(row.values()) for row in table.to_pylist()]
                insert_sql = build_insert_statement(target.table, table.column_names)
                execute_values(cursor, insert_sql, rows, page_size=INSERT_PAGE_SIZE)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    log.info("Loaded %s rows into %s for %s", table.num_rows if table else 0, target.table, trade_date)
