import pyspark.sql.functions as f

def get_partitions(spark_session, table_name: str):
  return list(map(lambda x: x["partition"].split("=")[-1], spark_session.sql(f"show partitions {table_name}").collect()))

def table_exists(spark_session, table_path):
  return spark_session.catalog.tableExists(table_path)

def register_date_patterns(spark_session, table_name, date_col_name: str, increment = 30, partitions_to_check = -1):
  pgio_table_partitions = get_partitions(spark, table_name)
  if (partitions_to_check != -1):
    pgio_table_partitions = pgio_table_partitions[partitions_to_check*-1:]

  for i in range(0, len(pgio_table_partitions), increment):
    partitions_to_process = pgio_table_partitions[i:i+increment]
    df = spark_session.table(table_name).where(
        f.col("dt_ingtao_ptcao").between(partitions_to_process[0], partitions_to_process[-1])
    ).select(
        f.lit(table_name).alias("table_name"),
        f.col("dt_ingtao_ptcao"),
        f.when(
            f.col(date_col_name).rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            f.lit("yyyy-MM-dd")
        ).when(
             f.col(date_col_name).rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            f.lit("dd.MM.yyyy")
        ).when(
            f.col(date_col_name).rlike(r'^\d{4}\.\d{2}\.\d{2}$'), 
            f.lit("yyyy.MM.dd")
        ).otherwise(
            f.col(date_col_name)
        ).alias("date_col")
    ).distinct()

    df.write.mode("append").saveAsTable("exp_dcps.tb_mpg_date_patterns")

register_date_patterns(spark, "pgio_table", "dpgto_ttlo")
