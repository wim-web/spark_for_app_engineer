from __future__ import print_function
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("not")
        exit(-1)
    
    spark = SparkSession.builder.appName("SparkStructuredStreamingNetworkWordcount").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    

    lines = spark.readStream.format("socket").option("host", sys.argv[1]).option("port", int(sys.argv[2])).load()
    
    words = lines.select(explode(split(lines.value, " ")).alias("word"))
    
    wordCount = words.groupBy("word").count()
    
    query = wordCount.writeStream.outputMode("complete").format("console").start()
    
    query.awaitTermination()
