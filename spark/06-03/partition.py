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
    ).withColumn("ts", from_unixtime(unix_timestamp("date", "yyyy/MM/dd HH:mm:ss"))).select(date_format("ts", "yyyyMM").alias("month"), "*")
    
    query = (df.writeStream
    .outputMode("append")
    .partitionBy("id", "month")
    .outputMode("append")
    .format("parquet")
    .option("path", __file__ + "/../partition")
    .option("checkpointLocation", __file__ + "/../checkpoint")
    .start()
    )
    
    query.awaitTermination()
