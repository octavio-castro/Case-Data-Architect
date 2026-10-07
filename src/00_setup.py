from pyspark.sql.functions import (
    col, lit, to_timestamp, month, hour, avg, sum, count, round, format_string
)
# Criar catálogo e schema
spark.sql("CREATE CATALOG IF NOT EXISTS taxi_case")
spark.sql("CREATE SCHEMA IF NOT EXISTS taxi_case.landing")

# Criar volumes para cada tipo de taxi
spark.sql("CREATE VOLUME IF NOT EXISTS taxi_case.landing.yellow")
spark.sql("CREATE VOLUME IF NOT EXISTS taxi_case.landing.green")
spark.sql("CREATE VOLUME IF NOT EXISTS taxi_case.landing.fhv")
spark.sql("CREATE VOLUME IF NOT EXISTS taxi_case.landing.fhvhv")
