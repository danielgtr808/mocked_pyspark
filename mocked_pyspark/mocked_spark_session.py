from .mocked_session_exception import MockedSessionException
from pyspark.sql import SparkSession

class MockedSparkSession():

  def __init__(self, spark_session: SparkSession, mocked_tables):
    self.spark_session = spark_session
    self.mocked_tables = mocked_tables
    self.database_in_use: str = "default"

    self.re_implemented_methods: dict = {
        "sql": self.__sql,
        "table": self.__table
    }

  def __sql(self, query: str):
    if (not query.lower().startswith("show partitions")):
        raise MockedSessionException("Only \"show partitions\" sql queries are supported")

    table_name = query[16:].strip()

    mocked_table = self.mocked_tables[table_name]
    if (mocked_table["partition_by"] == []):
        raise MockedSessionException(f"The table \"{table_name}\" is not partitioned")

    table = self.__table(table_name)
    return table.select(mocked_table["partition_by"]).distinct().orderBy(
        mocked_table["partition_by"]
    )

  def __table(self, table_name: str):
    if table_name in self.mocked_tables:
        return self.spark_session.createDataFrame(
            self.mocked_tables[table_name]["data"],
            schema = self.mocked_tables[table_name]["struct"]
        )
    raise MockedSessionException(f"Table {table_name} not found")

  def __getattr__(self, name):
    if name in self.re_implemented_methods.keys():
        return self.re_implemented_methods[name]
    
    return getattr(self.spark_session, name)