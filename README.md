# Oracle DWAF_HIS CDC → Kafka 파이프라인

이 프로젝트는 Oracle의 `APPUSER.DWAF_HIS` 테이블에서 새로 발생한 `INSERT` 이벤트만 캡처하고,
이벤트의 `SCANINDEX`로 `APPUSER.RT` 테이블을 조회한 다음 아래 네 필드를 JSON으로 만들어
Kafka의 `dwaf-file-events` 토픽에 발행하는 테스트 파이프라인입니다.

- `filepath`
- `filename`
- `sublot_id`
- `waf_id`

예상 출력은 다음과 같습니다.

```json
{"filepath":"/data/images/lot-a","filename":"wafer-1001.tif","sublot_id":"SUBLOT-A","waf_id":"WAF-001"}
```

## 사용 버전

- Apache Flink 1.20.5
- Apache Flink CDC Oracle 커넥터 3.6.0-1.20
- Flink Kafka 커넥터 3.4.0-1.20
- Apache Kafka 3.9.1(단일 노드 KRaft 모드)
- Oracle Database XE 21c 테스트 이미지

Kubernetes 네임스페이스는 `flink-cdc-demo`를 사용합니다.

## 영구 저장 경로

테스트 환경의 데이터를 유지하기 위해 다음 호스트 경로를 사용합니다.

- `cluster02:/var/lib/flink-cdc-demo/oracle`: Oracle 데이터
- `cluster02:/var/lib/flink-cdc-demo/kafka`: Kafka 데이터
- `cluster01:/var/lib/flink-cdc-demo/job`: Flink 잡 파일
- `cluster01:/var/lib/flink-cdc-demo/checkpoints`: Flink 체크포인트

## 처리 방식

Flink CDC의 Oracle `SourceFunction`은 병렬도 1로 실행됩니다. 최초 배포 시 초기 스냅샷을
수행하여 Debezium 스키마 이력을 생성하지만, 역직렬화 단계에서 스냅샷(`r`), 수정(`u`),
삭제(`d`) 이벤트를 제외합니다. 따라서 파이프라인 하류에는 새로 발생한 등록(`c`) 이벤트만
전달됩니다.

Oracle의 제약 조건 없는 `NUMBER` 값은 Debezium에서 문자열로 처리합니다. 이후 RT 테이블을
조회할 때 `BigDecimal`로 변환하므로 `SCANINDEX`를 JDBC 조회 키로 안전하게 사용할 수 있습니다.

처리 흐름은 다음과 같습니다.

1. `DWAF_HIS`의 신규 `INSERT` 이벤트를 감지합니다.
2. 이벤트에서 `SCANINDEX`, `SUBLOT_ID`, `WAF_ID`를 추출합니다.
3. `SCANINDEX`로 `RT` 테이블의 `FILEPATH`, `FILENAME`을 조회합니다.
4. 네 필드를 JSON으로 직렬화합니다.
5. Kafka `dwaf-file-events` 토픽에 `AT_LEAST_ONCE` 방식으로 전송합니다.

## 빌드 및 배포 상태 확인

```shell
mvn clean package
kubectl -n flink-cdc-demo get flinkdeployment oracle-dwaf-to-kafka -o wide
```

Flink 잡 상태가 `RUNNING / STABLE`이 될 때까지 기다립니다.

## 반복 테스트

다음 명령으로 테스트 데이터를 Oracle에 입력합니다.

```shell
kubectl -n flink-cdc-demo exec -i deployment/oracle -- \
  bash -lc 'sqlplus -s "APPUSER/$APP_USER_PASSWORD@//localhost:1521/XEPDB1" @/dev/stdin' \
  < sql/test-insert.sql
```

Kafka 토픽에서 결과 메시지를 확인합니다.

```shell
kubectl -n flink-cdc-demo exec kafka-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic dwaf-file-events \
  --from-beginning --max-messages 1 --timeout-ms 60000
```

`sql/test-insert.sql`은 동일한 테스트 행을 먼저 삭제한 뒤 다시 등록합니다. 파이프라인은 삭제
이벤트를 무시하므로 같은 테스트를 반복해서 실행할 수 있습니다.
