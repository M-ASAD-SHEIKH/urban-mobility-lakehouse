from pyspark import pipelines as dp          
from pyspark.sql.functions import col, when  


@dp.temporary_view()                                                    
def trips_joined():                                                     
    trips = spark.read.table("mobility.bronze.yellow_trips_raw")        
    zones = spark.read.table("mobility.bronze.zone_lookup_raw")         
    return trips.join(zones, trips["PULocationID"] == zones["LocationID"], "left")


@dp.temporary_view()
def trips_checked():
    df = spark.read.table("trips_joined")
    df = df.withColumn("pickup_time", col("tpep_pickup_datetime").cast("timestamp"))
    df = df.withColumn("dropoff_time", col("tpep_dropoff_datetime").cast("timestamp"))
    df = df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
    df = df.withColumn("reject_reason",
        when(col("pickup_time").isNull(), "MISSING_TIMESTAMP")
        .when((col("pickup_time") < "2024-01-01") | (col("pickup_time") >= "2024-04-01"), "DATE_OUT_OF_RANGE")
        .when(col("Zone").isNull(), "INVALID_PICKUP_ZONE")
        .when(col("dropoff_time") <= col("pickup_time"), "DROPOFF_NOT_AFTER_PICKUP")
        .when(col("fare_amount") < 0, "NEGATIVE_FARE"))
    return df

@dp.materialized_view(name="mobility.silver.trips_clean")     
def trips_clean():                                            
    return (spark.read.table("trips_checked")                 
        .filter(col("reject_reason").isNull())                
        .drop("reject_reason"))                               
    

@dp.materialized_view(name="mobility.silver.trips_quarantine")   
def trips_quarantine():                                          
    return (spark.read.table("trips_checked")                    
        .filter(col("reject_reason").isNotNull()))               




