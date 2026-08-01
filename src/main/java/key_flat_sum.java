import FlinkWaterMark.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class key_flat_sum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id=300", 3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L))
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return !value.user.equals("Alice");
                    }
                })
                .flatMap(new FlatMapFunction<Event, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(Event value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value.user,1));
                    }
                })
                        .keyBy(new KeySelector<Tuple2<String, Integer>, Integer>() {
                            @Override
                            public Integer getKey(Tuple2<String, Integer> value) throws Exception {
                                return value.f1;
                            }
                        })
                                .sum(1).print();
        env.execute();







    }
}
