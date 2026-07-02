import flink_to_out.Cat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class final_to_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers","master:9092");

        env.addSource(new FlinkKafkaConsumer<String>("clicks",new SimpleStringSchema(),pro))
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        String[] strings = value.split(",");
                        String name = strings[0];
                        int age = Integer.parseInt(strings[1]);
                        Long weight = Long.parseLong(strings[2]);
                        return new Cat(age,name,weight).toString();






                    }
                })
                .addSink(new FlinkKafkaProducer<String>("master:9092","events",new SimpleStringSchema()));

        env.execute();









    }



}
