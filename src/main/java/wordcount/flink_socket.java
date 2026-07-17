package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flink_socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> line = env.socketTextStream("master", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> word_one_DS = line.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> word_one = Tuple2.of(word, 1);
                    collector.collect(word_one);
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> word_one_keyed = word_one_DS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;

            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = word_one_keyed.sum(1);

        sum.print();

        env.execute();


    }
}
