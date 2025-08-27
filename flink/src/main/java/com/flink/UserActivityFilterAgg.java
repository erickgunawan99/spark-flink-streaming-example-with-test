package com.flink;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.time.Duration;

public class UserActivityFilterAgg {

    // POJOs
    public static class Event {
        public String event_type;
        public String url;
    }

    public static class UserActivity {
        public String id;
        public long date; // parsed from JSON
        public Event event;
    }
    public static class MyWindowFunction implements WindowFunction<
        Tuple4<String, String, Timestamp, Integer>, // Input type
        Tuple5<String, String, Timestamp, Timestamp, Integer>, // Output type
        Tuple2<String, String>, // Key type
        TimeWindow> {

         @Override
         public void apply(Tuple2<String, String> key,
                           TimeWindow window,
                           Iterable<Tuple4<String, String, Timestamp, Integer>> input,
                           Collector<Tuple5<String, String, Timestamp, Timestamp, Integer>> out) {

                int sum = 0;
                for (Tuple4<String, String, Timestamp, Integer> record : input) {
                    sum += record.f3;
                }

                Timestamp windowStart = new Timestamp(window.getStart());
                Timestamp windowEnd = new Timestamp(window.getEnd());

                out.collect(Tuple5.of(key.f0, key.f1, windowStart, windowEnd, sum));
        }
}

    public static void main(String[] args) throws Exception {
        final String user = System.getenv("DB_USER");
        final String password = System.getenv("DB_PASSWORD");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        // Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user_activity")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<UserActivity> activities = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map((MapFunction<String, UserActivity>) json -> mapper.readValue(json, UserActivity.class))
                .returns(UserActivity.class)
                // assign event-time + watermark (10s lateness)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserActivity>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, ts) -> event.date
       
                         ));
                        // truncate timestamp seconds and milliseconds to 0 so its grouped by minute
                        // LocalDateTime truncated = new Timestamp(ua.date).toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
                        // Timestamp truncatedTs = Timestamp.valueOf(truncated);
        // (id, event_type, count) aggregation
        DataStream<Tuple5<String, String, Timestamp, Timestamp, Integer>> aggregated = activities
                .filter(ua -> ua.event != null &&
                              (ua.event.event_type.equalsIgnoreCase("liked")
                               || ua.event.event_type.equalsIgnoreCase("commented")))
                .map(ua -> {
                    return Tuple4.of(ua.id, ua.event.event_type.toLowerCase(), new Timestamp(ua.date), 1);
                }) //truncate ua.date is truncated to minute precision, so all events in the same minute will have the same key.
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.SQL_TIMESTAMP, Types.INT))
                .keyBy(t -> Tuple2.of(t.f0, t.f1), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}))  // composite key id+event_type
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .apply(new MyWindowFunction(),
                         Types.TUPLE(Types.STRING, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.INT));

        // DataStream<Tuple3<String, String, Integer>> counts = activities
        //         .filter(ua -> ua.event != null &&
        //                       ("liked".equalsIgnoreCase(ua.event.event_type) ||
        //                        "commented".equalsIgnoreCase(ua.event.event_type)))
        //         .map(ua -> Tuple3.of(ua.id, ua.event.event_type, 1))
        //         .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
        //         .keyBy(t -> Tuple2.of(t.f0, t.f1)) // group by (id, event_type)
        //         .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
        //         .reduce((t1, t2) -> Tuple3.of(t1.f0, t1.f1, t1.f2 + t2.f2));


        // Sink into PostgreSQL
        aggregated.addSink(JdbcSink.sink(
                "INSERT INTO flink.user_activity_agg (user_id, event_type, window_start, window_end, total_count) VALUES (?, ?, ?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.f0); // user_id
                    ps.setString(2, t.f1);
                    ps.setTimestamp(3, t.f2);
                    ps.setTimestamp(4, t.f3);
                    ps.setInt(5, t.f4);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(20)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("org.postgresql.Driver")
                        .withUrl("jdbc:postgresql://localhost:5433/test_db")
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        ));

        env.execute("User Activity Aggregation to Postgres");
    }
}
