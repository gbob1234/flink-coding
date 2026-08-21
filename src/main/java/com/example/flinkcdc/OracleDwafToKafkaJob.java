package com.example.flinkcdc;

import io.debezium.data.Envelope;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.OracleSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/**
 * APPUSER.DWAF_HIS의 INSERT 이벤트를 감지하고 SCANINDEX로 APPUSER.RT를 조회한 뒤,
 * filepath/filename/sublot_id/waf_id를 JSON으로 만들어 Kafka에 발행하는 Flink 잡이다.
 */
public final class OracleDwafToKafkaJob {
    private OracleDwafToKafkaJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String oracleHost = parameters.get("oracle-host", "oracle.flink-cdc-demo.svc.cluster.local");
        int oraclePort = parameters.getInt("oracle-port", 1521);
        String database = parameters.get("oracle-database", "XE");
        String pdb = parameters.get("oracle-pdb", "XEPDB1");
        String schema = parameters.get("oracle-schema", "APPUSER").toUpperCase(Locale.ROOT);
        String cdcUser = requiredEnv("ORACLE_CDC_USER");
        String cdcPassword = requiredEnv("ORACLE_CDC_PASSWORD");
        String appUser = parameters.get("oracle-app-user", schema);
        String appPassword = requiredEnv("ORACLE_APP_PASSWORD");
        String kafkaBootstrap = parameters.get(
                "kafka-bootstrap", "kafka.flink-cdc-demo.svc.cluster.local:9092");
        String kafkaTopic = parameters.get("kafka-topic", "dwaf-file-events");

        // Oracle CDC 소스가 단일 스레드로 동작하므로 전체 잡의 기본 병렬도도 1로 맞춘다.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);
        env.setParallelism(1);

        // 제약 조건 없는 Oracle NUMBER를 문자열로 받아 정밀도 손실 없이 SCANINDEX를 처리한다.
        Properties debezium = new Properties();
        debezium.setProperty("database.pdb.name", pdb);
        debezium.setProperty("decimal.handling.mode", "string");
        debezium.setProperty("log.mining.strategy", "online_catalog");
        debezium.setProperty("heartbeat.interval.ms", "10000");

        DataStream<DwafInsert> inserts = env.addSource(
                        OracleSource.<DwafInsert>builder()
                                .hostname(oracleHost)
                                .port(oraclePort)
                                .database(database)
                                .schemaList(schema)
                                .tableList(schema + ".DWAF_HIS")
                                .username(cdcUser)
                                .password(cdcPassword)
                                // 초기 스냅샷으로 Debezium 스키마 이력을 만든다.
                                // 스냅샷 레코드("r")는 역직렬화 단계에서 버리므로
                                // 이후에 새로 등록된 DWAF_HIS 데이터만 하류로 전달된다.
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debezium)
                                .deserializer(new InsertOnlyDeserializer())
                                .build())
                .name("oracle-dwaf-his-cdc")
                .uid("oracle-dwaf-his-cdc");

        // CDC 이벤트의 SCANINDEX로 RT 테이블을 조회하고 Kafka에 보낼 JSON을 생성한다.
        String jdbcUrl = "jdbc:oracle:thin:@//" + oracleHost + ":" + oraclePort + "/" + pdb;
        DataStream<String> messages = inserts
                .flatMap(new RtLookup(jdbcUrl, appUser, appPassword, schema))
                .name("lookup-rt-by-scanindex")
                .uid("lookup-rt-by-scanindex")
                .map(JoinedEvent::toJson)
                .returns(TypeInformation.of(String.class))
                .name("serialize-json")
                .uid("serialize-json");

        // 체크포인트를 기준으로 최소 한 번 이상 전달되도록 Kafka Sink를 구성한다.
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        messages.sinkTo(sink).name("kafka-dwaf-file-events").uid("kafka-dwaf-file-events");
        env.execute("Oracle DWAF_HIS insert to Kafka");
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable is missing: " + name);
        }
        return value;
    }

    /** DWAF_HIS의 신규 등록 이벤트에서 파이프라인에 필요한 값만 담는다. */
    public static final class DwafInsert implements Serializable {
        public String scanIndex;
        public String sublotId;
        public String wafId;

        public DwafInsert() {}

        public DwafInsert(String scanIndex, String sublotId, String wafId) {
            this.scanIndex = scanIndex;
            this.sublotId = sublotId;
            this.wafId = wafId;
        }
    }

    /** RT 조회 결과와 DWAF_HIS 이벤트를 결합한 Kafka 메시지 모델이다. */
    public static final class JoinedEvent implements Serializable {
        public String filepath;
        public String filename;
        public String sublotId;
        public String wafId;

        public JoinedEvent() {}

        public JoinedEvent(String filepath, String filename, String sublotId, String wafId) {
            this.filepath = filepath;
            this.filename = filename;
            this.sublotId = sublotId;
            this.wafId = wafId;
        }

        /** 외부 JSON 라이브러리 없이 고정된 메시지 스키마로 직렬화한다. */
        public String toJson() {
            return "{\"filepath\":\"" + escape(filepath)
                    + "\",\"filename\":\"" + escape(filename)
                    + "\",\"sublot_id\":\"" + escape(sublotId)
                    + "\",\"waf_id\":\"" + escape(wafId) + "\"}";
        }

        /** 문자열 제어 문자를 JSON 표준 이스케이프 형식으로 변환한다. */
        private static String escape(String value) {
            if (value == null) {
                return "";
            }
            StringBuilder escaped = new StringBuilder(value.length() + 16);
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '\\': escaped.append("\\\\"); break;
                    case '"': escaped.append("\\\""); break;
                    case '\n': escaped.append("\\n"); break;
                    case '\r': escaped.append("\\r"); break;
                    case '\t': escaped.append("\\t"); break;
                    default:
                        if (c < 0x20) {
                            escaped.append(String.format("\\u%04x", (int) c));
                        } else {
                            escaped.append(c);
                        }
                }
            }
            return escaped.toString();
        }
    }

    /** Debezium CREATE(Oracle INSERT) 이벤트만 내보내고 스냅샷 READ 등은 무시한다. */
    public static final class InsertOnlyDeserializer
            implements DebeziumDeserializationSchema<DwafInsert> {
        @Override
        public void deserialize(SourceRecord record, Collector<DwafInsert> out) {
            if (!(record.value() instanceof Struct)) {
                return;
            }
            Struct envelope = (Struct) record.value();
            // Debezium 연산 코드 "c"만 INSERT이며 "r", "u", "d"는 처리하지 않는다.
            String operation = stringField(envelope, Envelope.FieldName.OPERATION);
            if (!"c".equals(operation)) {
                return;
            }
            Object afterValue = field(envelope, Envelope.FieldName.AFTER);
            if (!(afterValue instanceof Struct)) {
                return;
            }
            Struct after = (Struct) afterValue;
            String scanIndex = decimalString(field(after, "SCANINDEX"));
            if (scanIndex == null || scanIndex.isBlank()) {
                return;
            }
            out.collect(new DwafInsert(
                    scanIndex,
                    text(field(after, "SUBLOT_ID")),
                    text(field(after, "WAF_ID"))));
        }

        @Override
        public TypeInformation<DwafInsert> getProducedType() {
            return TypeInformation.of(DwafInsert.class);
        }

        private static String stringField(Struct struct, String name) {
            return text(field(struct, name));
        }

        private static Object field(Struct struct, String name) {
            if (struct == null || struct.schema() == null) {
                return null;
            }
            for (Field candidate : struct.schema().fields()) {
                if (candidate.name().equalsIgnoreCase(name)) {
                    return struct.get(candidate);
                }
            }
            return null;
        }

        private static String decimalString(Object value) {
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toPlainString();
            }
            return text(value);
        }

        private static String text(Object value) {
            return value == null ? null : value.toString();
        }
    }

    /** SCANINDEX로 RT 테이블을 조회하여 Kafka에 보낼 결합 이벤트를 생성한다. */
    public static final class RtLookup extends RichFlatMapFunction<DwafInsert, JoinedEvent> {
        private static final Logger LOG = LoggerFactory.getLogger(RtLookup.class);

        private final String jdbcUrl;
        private final String username;
        private final String password;
        private final String schema;
        // JDBC 객체는 Flink 상태로 직렬화하지 않고 각 TaskManager에서 open 시 생성한다.
        private transient Connection connection;
        private transient PreparedStatement statement;

        public RtLookup(String jdbcUrl, String username, String password, String schema) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.schema = schema;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("oracle.jdbc.OracleDriver");
            connect();
        }

        /** 기존 자원을 정리한 뒤 조회용 연결과 PreparedStatement를 새로 만든다. */
        private void connect() throws SQLException {
            closeJdbc();
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            statement = connection.prepareStatement(
                    "SELECT FILEPATH, FILENAME FROM " + schema + ".RT WHERE SCANINDEX = ?");
        }

        @Override
        public void flatMap(DwafInsert input, Collector<JoinedEvent> out) throws Exception {
            try {
                lookup(input, out);
            } catch (SQLException firstFailure) {
                // 일시적인 연결 끊김은 한 번 재접속하여 동일 이벤트 조회를 다시 시도한다.
                LOG.warn("RT lookup failed; reconnecting once", firstFailure);
                connect();
                lookup(input, out);
            }
        }

        private void lookup(DwafInsert input, Collector<JoinedEvent> out) throws SQLException {
            statement.clearParameters();
            statement.setBigDecimal(1, new BigDecimal(input.scanIndex));
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    out.collect(new JoinedEvent(
                            result.getString("FILEPATH"),
                            result.getString("FILENAME"),
                            input.sublotId,
                            input.wafId));
                } else {
                    // RT에 대응 행이 없으면 불완전한 메시지를 만들지 않고 이벤트를 건너뛴다.
                    LOG.warn("No RT row found for SCANINDEX={}; event skipped", input.scanIndex);
                }
            }
        }

        @Override
        public void close() {
            closeJdbc();
        }

        private void closeJdbc() {
            if (statement != null) {
                try { statement.close(); } catch (SQLException ignored) { }
                statement = null;
            }
            if (connection != null) {
                try { connection.close(); } catch (SQLException ignored) { }
                connection = null;
            }
        }
    }
}
