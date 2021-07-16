from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("not")
        exit(-1)
    
    sc = SparkContext(appName="SparkStreamingNetworkWordcount")
    sc.setLogLevel("WARN")
    
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("./checkpoint")
    
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    
    def update_count(new_key, last_sum):
        return sum(new_key) + (last_sum or 0)
    
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).updateStateByKey(update_count)
    
    counts.pprint()
    
    ssc.start()
    
    ssc.awaitTermination()
