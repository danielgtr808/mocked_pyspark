# Mocked PySpark

The objective of this project is to create a library that wraps arround the PySpark API to make it more testable 

With "testable", it means that through the library, the user will be able to mock tables and mimic some sql commands.


## Mocking a table

The code bellow shows how to mimic a table with a partitioning on the fields "column_1" and "column_2"

```python
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
            t.StructField("column_1", t.IntegerType(), True),
            t.StructField("column_2", t.IntegerType(), True),
            t.StructField("column_3", t.FloatType(), True)
        ]),
        "partition_by": ["column_1", "column_2"]
    }
}

spark_session = SparkSession.builder.getOrCreate()
mocked_spark_session = MockedSparkSession(spark_session, mocked_tables)
```


