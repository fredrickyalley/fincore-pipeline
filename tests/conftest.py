import os
import shutil
from collections.abc import Iterator

import pytest

JAVA_AVAILABLE = shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def spark() -> Iterator["SparkSession"]:  # type: ignore[name-defined]  # noqa: F821
    if not JAVA_AVAILABLE:
        pytest.skip("Spark tests need a JVM (set JAVA_HOME or put java on PATH)")
    from fincore_etl.session import build_spark_session

    session = build_spark_session("fincore-etl-tests", master="local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
