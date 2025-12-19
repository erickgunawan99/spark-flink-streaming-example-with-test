Stream Testing Lab: Spark vs. Flink
This repository compares two of the most popular stream processing engines—Apache Spark (PySpark) and Apache Flink (Java)—by implementing the same streaming logic in both.

The core focus of this project is demonstrating tests for streaming transformations (filtering and aggregation) using Chispa for Spark and JUnit for Flink.

🏗️ Project Architecture
Data Generation: A Python script in /generate_data streams JSON events to Apache Kafka.

Stream Processing:

Spark: PySpark Structured Streaming reads from Kafka, filters data, and sinks to PostgreSQL.

Flink: Flink (Java) DataStream API reads from Kafka, applies logic, and sinks to PostgreSQL.

Validation: Both engines feature dedicated test suites to validate business logic without requiring a live Kafka cluster.

-  Flink Testing: Integrated MiniCluster
Flink test (located in /flink/src/test) utilize the MiniClusterWithClientResource. This allows you to run a real Flink execution environment within a JUnit lifecycle.

What is it testing: The filtering and event-time aggregation function

Logic Isolation: The test validates a WindowFunction that aggregates user activities (likes/comments) into 1-minute tumbling windows.

Data Validation: Instead of complex external sinks, we use aggregated.executeAndCollect(). This stream-to-iterator pattern allows the test to "wait" for the stream to finish and collect results into a standard Java List for assertions.

Time Control: By using WatermarkStrategy and TumblingEventTimeWindows, the test proves that the aggregation logic correctly handles event-time boundaries.

-  Spark Testing: Fast Schema Validation with Chispa
Spark test (located in /spark/tests) use Chispa, the industry standard for PySpark unit testing.

What is it testing: The filtering and event-time aggregation function

DataFrame Comparison: chispa.assert_df_equality is used to compare the entire transformation output against an expected "golden" dataset.

No Cluster Needed: Tests run locally using a local SparkSession, ensuring that filtering and stateful aggregation logic is verified before the code ever touches a production cluster.
