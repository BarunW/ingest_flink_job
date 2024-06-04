package consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import models.UserInteractionData;
import models.UserTimeSpent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sink.JdbcUserTimeSpentSink;
import sink.Sink;
import sink.SinkFactory;


import java.time.Duration;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws Exception {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/user_interaction_data";
        String jdbcUser = "postgres";
        String jdbcPassword = "mysecretpassword";


        // Stream Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Properties
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "localhost:9092");
        consumerConfig.setProperty("group.id", "flink-user-interaction-data-consumer-group");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperties(consumerConfig)
                .setTopics("user-interaction-data")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env
                .fromSource(
                        source,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                        "User data interaction kafka source"
                );
        // rawStream.print();


        // convert the data from SimpleStringSchema to UserInteractionData using jackson
        ObjectMapper objectMapper = new ObjectMapper();
        DataStream<UserInteractionData> userInteractionDataStream =
                rawStream.map(new MapFunction<String, UserInteractionData>() {
                    @Override
                    public UserInteractionData map(String value) throws Exception {
                        return objectMapper.readValue(value, UserInteractionData.class);
                    }
                });

        // apply transformation
        SingleOutputStreamOperator<UserTimeSpent> userTimeSpentStream = userInteractionDataStream
                .keyBy(event -> event.UserId)
                // window the stream in 5 min chunk non-overlapping
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .apply(new WindowFunction<UserInteractionData, UserTimeSpent, String, TimeWindow>() {
                    @Override
                    public void apply(String userId, TimeWindow window, Iterable<UserInteractionData> events, Collector<UserTimeSpent> out) {
                        long timeSpent = 0;
                        for (UserInteractionData event : events) {
                            timeSpent += event.SessionDuration;
                        }
                        out.collect(new UserTimeSpent(userId, timeSpent));
                    }
                });
        // JDBC Sink
        Sink<UserTimeSpent> jdbcSink = SinkFactory.createJdbcSink(jdbcUrl, jdbcUser, jdbcPassword);

        // adding the sink to userTimeSpentStream
        userTimeSpentStream.addSink((JdbcUserTimeSpentSink) jdbcSink);
        env.execute();
    }
}