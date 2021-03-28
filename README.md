# Enriching Kafka Stream with Another Stream Using Flink

[![codecov](https://codecov.io/gh/ramottamado/postgres-cdc-flink/branch/main/graph/badge.svg?token=ZVUN5AP7WA)](https://codecov.io/gh/ramottamado/postgres-cdc-flink)

## Environment Setup
1. Install PostgreSQL 11+
2. Setup PostgreSQL to allow Debezium to CDC using pgoutput. Reference [here](https://debezium.io/documentation/reference/1.1/connectors/postgresql.html)
3. Setup Apache Kafka (with Kafka Connect) on your machine/cluster
4. Install Debezium PostgreSQL connector from [here](https://debezium.io/documentation/reference/install.html)
5. Run Apache Kafka & Kafka Connect
6. Create table `transactions` and `customers` in PostgreSQL (SQL file in [here](sql/tables.sql))
7. Create POST request to your Kafka Connect REST interface with request body as below
```json
{
  "name": "postgres_cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "<host>",
    "database.port": "<port>",
    "database.user": "<username>",
    "database.password": "****",
    "database.dbname" : "<dbname>",
    "database.server.name": "<servername>",
    "table.whitelist": "<schema>.customers,<schema>.transactions",
    "plugin.name": "pgoutput"
  }
}
```
8. Run the jar

## Streaming Job Available Parameters
1. `--checkpoint-path`: path to save Flink's checkpoints.
2. `--debug-result-stream`: whether to debug result stream to the console or not
3. `--environment`: environment to run the app
4. `--auto-offset-reset`: Kafka `auto.offset.reset` parameter
5. `--boostrap-server`: Kafka bootstrap servers
6. `--consumer-group-id`: Kafka consumer group ID
7. `--offset-strategy`: whether to get earliest or latest offset from Kafka
8. `--source-topic-1`: Kafka transactions stream name
9. `--source-topic-2`: Kafka customers stream name
10. `--target-topic`: target topic name to publish enriched data
11. `--properties-file`: properties file to load parameters from