package review;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class fromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","master:9092");
        DataStreamSource<String> test = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        test.print();
        env.execute();



    }

}
