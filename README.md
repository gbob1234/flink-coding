# Oracle DWAF_HIS CDC to Kafka

This test pipeline captures only new `INSERT` events from Oracle
`APPUSER.DWAF_HIS`, looks up `APPUSER.RT` by `SCANINDEX`, and writes JSON to
Kafka topic `dwaf-file-events`.

Expected output:

```json
{"filepath":"/data/images/lot-a","filename":"wafer-1001.tif","sublot_id":"SUBLOT-A","waf_id":"WAF-001"}
```

Pinned versions:

- Apache Flink 1.20.5
- Apache Flink CDC Oracle connector 3.6.0-1.20
- Flink Kafka connector 3.4.0-1.20
- Apache Kafka 3.9.1 in single-node KRaft mode
- Oracle Database XE 21c test image

Kubernetes namespace: `flink-cdc-demo`.

The demo uses host paths for persistence:

- `cluster02:/var/lib/flink-cdc-demo/oracle`
- `cluster02:/var/lib/flink-cdc-demo/kafka`
- `cluster01:/var/lib/flink-cdc-demo/job`
- `cluster01:/var/lib/flink-cdc-demo/checkpoints`

The job uses Flink CDC's single-threaded Oracle `SourceFunction`. On a fresh
deployment it creates Debezium schema history with an initial snapshot, but the
deserializer drops snapshot (`r`), update, and delete events. Only new insert
(`c`) events reach the RT lookup and Kafka sink. Oracle `NUMBER` values use
Debezium string handling so an unconstrained `NUMBER` is safe as a JDBC lookup
key.

Build and check the deployment:

```shell
mvn clean package
kubectl -n flink-cdc-demo get flinkdeployment oracle-dwaf-to-kafka -o wide
```

Run the repeatable test after the job is `RUNNING / STABLE`:

```shell
kubectl -n flink-cdc-demo exec -i deployment/oracle -- \
  bash -lc 'sqlplus -s "APPUSER/$APP_USER_PASSWORD@//localhost:1521/XEPDB1" @/dev/stdin' \
  < sql/test-insert.sql

kubectl -n flink-cdc-demo exec kafka-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic dwaf-file-events \
  --from-beginning --max-messages 1 --timeout-ms 60000
```

`sql/test-insert.sql` deletes the prior demo row first. Delete events are
ignored, making the insert test repeatable.
