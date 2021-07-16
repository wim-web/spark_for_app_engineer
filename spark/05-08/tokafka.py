# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("join").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    kafkaDataFrame = (spark
    .readStream.
    format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor-data")
    .load())
    
    stringDF = kafkaDataFrame.selectExpr("CAST(value as STRING) as value")
    
    cooordSchema = StructType().add("lat", DoubleType()).add("lon", DoubleType())
    mainSchema = StructType().add("temperature", DoubleType()).add("humidity", DoubleType()).add("ph", DoubleType()).add("whc", DoubleType())
    schema = StructType().add("id", LongType()).add("date", StringType()).add("coord", cooordSchema).add("main", mainSchema)
    
    jsonDF = stringDF.select(from_json(stringDF.value, schema).alias("sensor_data"))
    
    df = jsonDF.select(
        col("sensor_data.id").alias("id"),
        col("sensor_data.date").alias("date"),
        col("sensor_data.coord.lat").alias("lat"),
        col("sensor_data.coord.lon").alias("lon"),
        col("sensor_data.main.temperature").alias("temperature"),
        col("sensor_data.main.humidity").alias("humidity"),
        col("sensor_data.main.ph").alias("ph"),
        col("sensor_data.main.whc").alias("whc")
    )
    
    columnDF = df.withColumnRenamed("id", "sensor_id")
    
    s = StructType().add("sensor_id", LongType()).add("field_id", StringType())
        
    csvDF = spark.read.format("com.databricks.spark.csv").schema(s).option("header", "true").load(__file__ + "/../../sensor_field.csv")
    
    joinedDF = columnDF.join(csvDF, "sensor_id", "leftouter")
    
    query = (joinedDF
    .selectExpr("to_json(struct(*)) as value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "joined-sensor-data")
    .option("checkpointLocation", __file__ + "/../checkpoint").start()
    )
    
    query.awaitTermination()
