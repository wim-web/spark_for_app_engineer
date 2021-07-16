# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def doubleString(str):
    return str + str
    
if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("udf").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    spark.udf.register("doubleString", doubleString, StringType())
    
    s = StructType().add("sensor_id", LongType()).add("field_id", StringType())
    
    csvDF = spark.read.format("com.databricks.spark.csv").schema(s).option("header", "true").load(__file__ + "/../../sensor_field.csv")
    csvDF.registerTempTable("sensor_master")
    
    udfDF = spark.sql("select sensor_id, field_id, doubleString(field_id) as double_field_id from sensor_master")
    
    udfDF.printSchema()
    udfDF.show()
