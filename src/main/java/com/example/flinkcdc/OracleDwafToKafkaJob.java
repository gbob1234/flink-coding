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
 * Captures INSERT events from APPUSER.DWAF_HIS, looks up APPUSER.RT by
 * SCANINDEX, and publishes filepath/filename/sublot_id/waf_id JSON to Kafka.
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);
        env.setParallelism(1);

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
                                // Build Debezium's schema history with an initial snapshot.
                                // InsertOnlyDeserializer discards snapshot ("r") records, so
                                // only new DWAF_HIS inserts are emitted downstream.
                                .startupOptions(StartupOptions.initial())
                                .debeziumProperties(debezium)
                                .deserializer(new InsertOnlyDeserializer())
                                .build())
                .name("oracle-dwaf-his-cdc")
                .uid("oracle-dwaf-his-cdc");

        String jdbcUrl = "jdbc:oracle:thin:@//" + oracleHost + ":" + oraclePort + "/" + pdb;
        DataStream<String> messages = inserts
                .flatMap(new RtLookup(jdbcUrl, appUser, appPassword, schema))
                .name("lookup-rt-by-scanindex")
                .uid("lookup-rt-by-scanindex")
                .map(JoinedEvent::toJson)
                .returns(TypeInformation.of(String.class))
                .name("serialize-json")
                .uid("serialize-json");

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

        public String toJson() {
            return "{\"filepath\":\"" + escape(filepath)
                    + "\",\"filename\":\"" + escape(filename)
                    + "\",\"sublot_id\":\"" + escape(sublotId)
                    + "\",\"waf_id\":\"" + escape(wafId) + "\"}";
        }

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

    /** Emits only Debezium CREATE (Oracle INSERT) events; snapshot READ events are ignored. */
    public static final class InsertOnlyDeserializer
            implements DebeziumDeserializationSchema<DwafInsert> {
        @Override
        public void deserialize(SourceRecord record, Collector<DwafInsert> out) {
            if (!(record.value() instanceof Struct)) {
                return;
            }
            Struct envelope = (Struct) record.value();
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

    public static final class RtLookup extends RichFlatMapFunction<DwafInsert, JoinedEvent> {
        private static final Logger LOG = LoggerFactory.getLogger(RtLookup.class);

        private final String jdbcUrl;
        private final String username;
        private final String password;
        private final String schema;
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
