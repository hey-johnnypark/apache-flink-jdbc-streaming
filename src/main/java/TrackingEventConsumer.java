import io.jp.Protos;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;

import java.sql.Types;
import java.util.Properties;


/**
 * Created by johnnypark on 13/03/2017.
 */
public class TrackingEventConsumer {

    private static final String TOPIC = "tracking";
    private static final String KAFKA_BOOTSTRAP = "192.168.1.4:9092";
    private static final int JDBC_BATCH_SIZE = 200;
    private static final String JDBC_USER = "root";
    private static final String JDBC_PASSWORD = "root";
    private static final String JDBC_DB = "stream";
    private static final String JDBC_TABLE = "tracking";
    private static final String JDBC_HOST = "localhost";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new FlinkKafkaConsumer010<byte[]>("tracking", new ByteSerialisationSchema(), createKafkaProperties()))
                .setParallelism(10)
                .map((bytes) -> Protos.TrackingEvent.parseFrom(bytes))
                .map(TrackingEventConsumer::createOutputFormat)
                .map(new MetricsMapper<>())
                .writeUsingOutputFormat(createJDBCSink())
                .setParallelism(10);
        env.execute();
    }

    private static Properties createKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP);
        return properties;
    }

    private static Row createOutputFormat(Protos.TrackingEvent event) {
        Row row = new Row(2);
        row.setField(0, event.getId());
        row.setField(1, event.getMessage());
        return row;
    }

    private static JDBCOutputFormat createJDBCSink() {
        return JDBCOutputFormat.buildJDBCOutputFormat()
                .setDBUrl(String.format("jdbc:mysql://%s/%s", JDBC_HOST, JDBC_DB))
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername(JDBC_USER)
                .setPassword(JDBC_PASSWORD)
                .setQuery(String.format("insert into %s (id, message) values (?,?)", JDBC_TABLE))
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR})
                .setBatchInterval(JDBC_BATCH_SIZE)
                .finish();
    }

}
