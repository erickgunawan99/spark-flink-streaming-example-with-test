package com.flink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
public class UserActivityInsert {
// POJO
    public class UserActivity {
        public String id;
        public Timestamp date;
        public Event event;

        public static class Event {
            public String event_type;
            public String url;
        }
    }
   

    public class FlinkKafkaJob {
    public static void main(String[] args) throws Exception {
        final String user = System.getenv("DB_USER");
        final String password = System.getenv("DB_PASSWORD");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user_activity")
                .setGroupId("flink-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source"
        );

        ObjectMapper mapper = new ObjectMapper();

        // 2. Parse JSON → POJO
        DataStream<UserActivity> events = kafkaStream.map(json -> 
            mapper.readValue(json, UserActivity.class)
        );

        // 3. Filter streams
        DataStream<UserActivity> importantEvents = events.filter(
            e -> e.event != null && (
                e.event.event_type.equalsIgnoreCase("liked") ||
                e.event.event_type.equalsIgnoreCase("bookmarked") ||
                e.event.event_type.equalsIgnoreCase("commented")
            )
        );

       // DataStream<UserActivity> viewedOnly = events.filter(
       //     e -> e.event != null && e.event.event_type.equalsIgnoreCase("viewed")
       // );

        // 4. Write importantEvents → JDBC (Postgres)
        importantEvents.addSink(
            JdbcSink.sink(
                "INSERT INTO flink.user_activity (id, date, event_type, url) VALUES (?, ?, ?, ?)",
                (ps, ua) -> {
                    ps.setString(1, ua.id);
                    ps.setTimestamp(2, ua.date);
                    ps.setString(3, ua.event.event_type);
                    ps.setString(4, ua.event.url);
                }, 
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(20)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/test_db")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(user)
                        .withPassword(password)
                        .build()
            )
        );

        env.execute("Flink Kafka → Postgres Job");
    }
    }
}