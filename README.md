# アプリケーションエンジニアのためのApache Spark入門の環境

https://www.amazon.co.jp/gp/product/B07RR8CL7R

fluentd + kafka + spark(+jupyter)を立ち上げられる

## kafka

立ち上げ

```
# 環境変数の設定(fish) bash等は読み替えてください
set -x DOCKER_HOST_IP (ipconfig getifaddr en0)
docker-compose up -d
```

topic作成

```
docker-compose exec kafka bash
/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --topic sample --partitions 1 --replication-factor 1
```

topic確認

```
docker-compose exec kafka bash
/opt/kafka/bin/kafka-topics.sh --list --zookeeper zookeeper:2181
```

consoleでtopicに送信

```
docker-compose exec kafka bash
/opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sample
```

consoleでtopicから受信

```
docker-compose exec kafka bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sample --from-beginning
```

## docker-compose

fluentdにhttp経由で送るtest

※結構ラグがある

```
curl -XPOST -d 'json={"id":123, "date":"2020-09-01 11:00", "coord":{"lon":123.21, "lat": 32.33}, "main":{"temparature":10.5, "humidity":99, "ph":8.8, "whc":98.8}}' http://localhost:24224/sensor.data
```

## streaming.py

host側(mac)

```
nc -lk 8889
```

docker

```
docker-compose up -d
docker-compose exec spark bash
cd /home/spark/05-01
/usr/local/spark/bin/spark-submit streaming.py host.docker.internal 8889
```

ホスト側で入力すれば反映される
（なんかエラー出るけど）

## create sensor data

cronがめんどくさいのでホスト側のpythonで一定間隔で出力する

```
python interval.py >> ./spark/sensor_data/sensor_data.log
```

# sensor_data -> fluentd -> kafka

コンソールでかくにん

```
docker-compose exec kafka bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sensor-data
# これはホスト側
python interval.py >> log/sensor_data/sensor_data.log
```

# sensor_data -> fluentd -> kafka -> spark

```
docker-compose exec kafka bash
/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka.py
```

```
python interval.py >> log/sensor_data/sensor_data.log
```

# csv

package指定しなくてもいけた
