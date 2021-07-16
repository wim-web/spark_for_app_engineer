# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("csv").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    s = StructType().add("sensor_id", LongType()).add("field_id", StringType())
    
    print(__file__)
    
    df = spark.read.format("com.databricks.spark.csv").schema(s).option("header", "true").load(__file__ + "/../../sensor_field.csv")
    
    df.show()
