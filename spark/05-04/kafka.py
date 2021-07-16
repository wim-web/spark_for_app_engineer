# coding: UTF-8
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # 1. "KafkatoConsole"というアプリケーション名称を持つSparkSessionを作成
    spark = SparkSession.builder.appName("KafkatoConsole").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. 192.168.33.10:9092でアクセス可能なKafkaの、`sensor-data` Topicからデータを取得する。
    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "sensor-data").load()

    # 3. Valueカラムを文字列に変換
    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    # 4. 文字列変換結果をコンソール出力
    query = stringFormattedDataFrame.writeStream.outputMode("append").format("console").start()

    # 5. 終了されるまで継続的に読み込みと出力を実行
    query.awaitTermination()
