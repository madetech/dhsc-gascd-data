# Databricks notebook source
from pyspark.sql.functions import explode, flatten, col, to_timestamp, expr, regexp_replace, when, first, lit
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import os
import pandas as pd

# COMMAND ----------

# Get environment variable
environment = os.getenv("ENV")

silver_restricted_path = "abfss://restricted@dapalphastsilver" + environment + ".dfs.core.windows.net/"
gold_restricted_path = "abfss://restricted@dapalphastgold" + environment + ".dfs.core.windows.net/"

# COMMAND ----------

main = spark.read.format("parquet").load(silver_restricted_path + "Capacity_Tracker/Generic/main.parquet")

# COMMAND ----------

main.createOrReplaceTempView("main")
provider_metrics = spark.sql(
    '''
    SELECT
        cqc_id,
        la_code,
        load_date_time,
        CAST(workforce_hours_agency AS DOUBLE) AS percentage_workforce_hours_agency_num,
        CAST(workforce_hours_paid + workforce_hours_agency AS DOUBLE) AS percentage_workforce_hours_agency_denom,
        CAST(workforce_hours_overtime AS DOUBLE) AS percentage_direct_workforce_hours_overtime_num,
        CAST(workforce_hours_paid AS DOUBLE) AS percentage_direct_workforce_hours_overtime_denom
    FROM main
    '''
)

# COMMAND ----------

metrics = [
    "percentage_workforce_hours_agency_num",
    "percentage_workforce_hours_agency_denom",
    "percentage_direct_workforce_hours_overtime_num",
    "percentage_direct_workforce_hours_overtime_denom"
]

# COMMAND ----------

stack_expr = "stack({}, {}) as (metric, value)".format(
    len(metrics),
    ", ".join([f"'{metric}', {metric}" for metric in metrics])
)

provider_metrics_long = provider_metrics.selectExpr(
    "cqc_id",
    "la_code",
    "load_date_time as date_time",
    stack_expr
)

provider_metrics_long = provider_metrics_long.withColumn(
    "metric_id",
    regexp_replace("metric", "_num|_denom", "")
)

provider_metrics_long = provider_metrics_long.withColumn(
    "type",
    when(provider_metrics_long.metric.endswith("_num"), "numerator")
    .when(provider_metrics_long.metric.endswith("_denom"), "denominator")
)

provider_metrics_long = provider_metrics_long.select('cqc_id', 'la_code', 'date_time','metric_id', 'type', 'value')

provider_metrics_wide = provider_metrics_long.groupBy("cqc_id", "la_code", "date_time", "metric_id") \
    .pivot("type") \
    .agg(first("value"))

provider_metrics_wide = provider_metrics_wide.select('cqc_id', 'la_code','date_time','metric_id', 'numerator', 'denominator')

display(provider_metrics_wide)

# COMMAND ----------

provider_metrics_wide.createOrReplaceTempView("provider_metrics_wide")

provider_metrics_table_provider = spark.sql("""
    SELECT
        metric_id,
        'daily' AS metric_date_type,
        to_date(date_time) AS metric_date,
        'provider' AS location_type,
        cqc_id AS location_id,
        numerator,
        denominator,
        CAST(NULL AS DOUBLE) AS data_point
    FROM provider_metrics_wide
""")

la_metrics_table = spark.sql("""
    SELECT
        metric_id,
        'daily' AS metric_date_type,
        to_date(date_time) AS metric_date,
        'local authority' AS location_type,
        la_code AS location_id,
        SUM(numerator) AS numerator,
        SUM(denominator) AS denominator,
        CAST(NULL AS DOUBLE) AS data_point
    FROM provider_metrics_wide
    GROUP BY
        metric_id,
        to_date(date_time),
        la_code
""")

metrics_table = provider_metrics_table_provider.union(la_metrics_table)

# COMMAND ----------

metrics_table.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(gold_restricted_path + "metrics_table.parquet")
