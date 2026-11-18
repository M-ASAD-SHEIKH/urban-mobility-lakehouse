from pyspark import pipelines as dp
from pyspark.sql import*
from pyspark.sql.window import Window

@dp.table(name="yellow_trips_raw",
          table_properties={"delta.feature.timestampNtz": "supported"})
def yellow_trips_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("abfss://landing-files@proj2storage.dfs.core.windows.net/yellow_taxi/")
    )

@dp.materialized_view(name="zone_lookup_raw")
def zone_lookup_raw():
    return (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("abfss://landing-files@proj2storage.dfs.core.windows.net/reference/")
    )