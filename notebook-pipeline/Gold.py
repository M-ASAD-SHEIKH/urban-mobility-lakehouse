from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="mobility.gold.trips_by_zone")     
def trips_by_zone():                                           
    return (spark.read.table("mobility.silver.trips_clean")   
        .groupBy("Zone", "Borough")                            
        .agg(F.count("*").alias("total_trips"),                
             F.round(F.avg("fare_amount"), 2).alias("avg_fare")))  
    


@dp.materialized_view(name="mobility.gold.trips_by_hour")
def trips_by_hour():
    return (spark.read.table("mobility.silver.trips_clean")
        .withColumn("pickup_hour", F.hour("pickup_time"))      
        .groupBy("pickup_hour")                                
        .agg(F.count("*").alias("total_trips")))               
    


@dp.materialized_view(name="mobility.gold.monthly_summary")
def monthly_summary():
    return (spark.read.table("mobility.silver.trips_clean")
        .withColumn("trip_month", F.date_format("pickup_time", "yyyy-MM"))   
        .groupBy("trip_month")                                               
        .agg(F.count("*").alias("total_trips"),
             F.round(F.sum("fare_amount"), 2).alias("total_fare")))          

