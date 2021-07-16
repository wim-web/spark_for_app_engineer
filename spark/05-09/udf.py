# coding: UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def doubleString(str):
    return str + str
    
if __name__ == "__main__":
    doubleStringUdf = udf(doubleString, StringType())
    
    spark = SparkSession.builder.appName("udf").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    s = StructType().add("sensor_id", LongType()).add("field_id", StringType())
    
    csvDF = spark.read.format("com.databricks.spark.csv").schema(s).option("header", "true").load(__file__ + "/../../sensor_field.csv")
    
    udfDF = csvDF.withColumn("double_field_id", doubleStringUdf(csvDF.field_id))
    
    udfDF.printSchema()
    udfDF.show()
