from pyspark.sql import SparkSession
from mocked_pyspark import MockedSparkSession

import pyspark.sql.types as t
import pyspark.sql.functions as f


def run_app(spark_session: SparkSession) -> None:
    table_1 = spark_session.table("database_name.table_1")
    table_1.show(truncate = False)

    table_1_partitions = spark_session.sql("show partitions database_name.table_1")
    table_1_partitions.show(truncate = False)

mocked_tables = {
    "database_name.table_1": {
        "data": [
            (1, 1, 100.0),
            (1, 2, 200.0),
            (1, 3, 300.0),
        ],
        "struct": t.StructType([
            t.StructField("cag_bcria", t.IntegerType(), True),
            t.StructField("ncta_bcria", t.IntegerType(), True),
            t.StructField("valor", t.FloatType(), True),
            t.StructField("cid_trans", t.StringType(), True),
        ]),
        "partition_by": ["cag_bcria", "ncta_bcria"]
    }
}

spark_session = SparkSession.builder.getOrCreate()
mocked_spark_session = MockedSparkSession(spark_session, mocked_tables)
