# apm-kafka-go

This is a small PoC for distributed tracing via [Elastic APM](https://www.elastic.co/guide/en/apm/guide/current/apm-overview.html) of Go applications sending messages
via [Apache Kafka](https://kafka.apache.org/).

## Running

Start the required backend services - Kafka, Elasticsearch, Kibana and APM - via 

```shell
docker-compose up -d
```

Build the producer and consumer via

```shell
make build # to build both applications

# OR build them individually
make build-producer
make build-consumer
```

Run both applications and watch the tracing information [in Kibana](http://localhost:5601/app/apm/services?rangeFrom=now-15m&rangeTo=now).

Finally, stop the applications and remove the containers

```shell
docker-compose down
```
