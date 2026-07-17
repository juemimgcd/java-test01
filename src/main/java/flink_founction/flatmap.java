package flink_founction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
public class flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<cat> cats = env.fromElements(
                new cat(12, "coco"),
                new cat(3, "dongdong"),
                new cat(2, "meme")
        );

        SingleOutputStreamOperator<String> cats_flatmap = cats.flatMap(new FlatMapFunction<cat, String>() {

            @Override
            public void flatMap(cat value, Collector<String> out) throws Exception {
                if ("coco".equals(value.getName())){
                    out.collect("this is coco");
                }
                else {
                    out.collect(value.getName());
                    out.collect(value.getAge()+"");
                }
            }
        });
        cats_flatmap.print();
        env.execute();

    }
}
