package com.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import java.util.Spliterator;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.apache.flink.util.Collector;

import java.util.Spliterators;

public class FlinkTest {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    // ------------------------------
    // Test POJOs
    // ------------------------------
    public static class Event {
        public String event_type;
        public String url;

        public Event() {}

        public Event(String event_type, String url) {
            this.event_type = event_type;
            this.url = url;
        }
    }

    public static class UserActivity {
        public String id;
        public long date;   // epoch millis
        public Event event;

        public UserActivity() {}

        public UserActivity(String id, long date, Event event) {
            this.id = id;
            this.date = date;
            this.event = event;
        }
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
    @Test
    public void testAggregationWithWindowBounds() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // ✅ All timestamps fall within the same 1-minute window
        long baseTime = LocalDateTime.of(2023, 8, 16, 12, 0)
                                     .toInstant(ZoneOffset.UTC)
                                     .toEpochMilli();

        List<UserActivity> input = List.of(
            new UserActivity("u1", baseTime + 5_000L, new Event("liked", "url1")),
            new UserActivity("u1", baseTime + 10_000L, new Event("liked", "url2")),
            new UserActivity("u1", baseTime + 30_000L, new Event("commented", "url3")),
            new UserActivity("u1", baseTime + 30_000L, new Event("viewed", "url4"))
        );

        DataStream<UserActivity> activities = env
            .fromData(input, TypeInformation.of(UserActivity.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserActivity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.date)
            );
        DataStream<Tuple5<String, String, Timestamp, Timestamp, Integer>> aggregated = activities
                .filter(ua -> ua.event != null &&
                              (ua.event.event_type.equalsIgnoreCase("liked")
                               || ua.event.event_type.equalsIgnoreCase("commented")))
                .map(ua -> { //Timestamp date_ts = new Timestamp(ua.date);
                    //LocalDateTime truncated = new Timestamp(ua.date).toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
                    //Timestamp truncatedTs = Timestamp.valueOf(truncated);
                    return Tuple4.of(ua.id, ua.event.event_type.toLowerCase(), new Timestamp(ua.date), 1);
                }) //truncate ua.date is truncated to minute precision, so all events in the same minute will have the same key.
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.SQL_TIMESTAMP, Types.INT))
                .keyBy(t -> Tuple2.of(t.f0, t.f1), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}))  // composite key id+event_type
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .apply(new MyWindowFunction(),
                         Types.TUPLE(Types.STRING, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.INT));
         /*                 
        DataStream<Tuple5<String, String, String, String, Integer>> agg =
            activities
                .filter(a -> a.event != null &&
                            ("liked".equalsIgnoreCase(a.event.event_type) ||
                             "commented".equalsIgnoreCase(a.event.event_type)))
                .map(a -> Tuple3.of(a.id, a.event.event_type, 1))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
                .keyBy(t -> t.f0 + "_" + t.f1)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .apply(new WindowFunction<
                        Tuple3<String, String, Integer>,
                        Tuple5<String, String, String, String, Integer>,
                        String,
                        TimeWindow>() {
                    @Override
                    public void apply(
                            String key,
                            TimeWindow window,
                            Iterable<Tuple3<String, String, Integer>> input,
                            Collector<Tuple5<String, String, String, String, Integer>> out) {
                        int sum = 0;
                        String id = null;
                        String eventType = null;
                        for (Tuple3<String, String, Integer> record : input) {
                            id = record.f0;
                            eventType = record.f1;
                            sum += record.f2;
                        }
                        String windowStart = new Timestamp(window.getStart()).toString();
                        String windowEnd = new Timestamp(window.getEnd()).toString();
                        out.collect(Tuple5.of(id, eventType, windowStart, windowEnd, sum));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple5<String, String, String, String, Integer>>() {}));
*/
        // ✅ Collect results
        List<Tuple5<String, String, Timestamp, Timestamp, Integer>> result;
        try (CloseableIterator<Tuple5<String, String, Timestamp, Timestamp, Integer>> it = aggregated.executeAndCollect()) {
            result = StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
            .collect(Collectors.toList());
        }


        // ✅ Expect: two aggregated rows (liked=2, commented=1)
        assertEquals(2, result.size());

        // Print to verify (optional)
        result.forEach(System.out::println);
    }
}