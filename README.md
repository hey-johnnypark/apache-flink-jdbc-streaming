# apache-flink-jdbc-streaming
Sample project for Apache Flink with Streaming Engine and JDBC Sink

## Setting
```
[[Source(Protobuf)] -> [Sink(Kafka)]] 
   |
   |   
[Kafka]
   |
   |   
[[Source (Kafka) -> Map(Protobuf, Row) -> Sink(JDBC)]]  
```
