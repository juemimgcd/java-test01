package flink_founction;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<cat> cats = env.fromElements(
                new cat(12, "coco"),
                new cat(3, "dongdong"),
                new cat(2, "meme")
        );
//        SingleOutputStreamOperator<cat> cats_filter = cats.filter(new FilterFunction<cat>() {
//            @Override
//            public boolean filter(cat value) throws Exception {
//                return "coco".equals(value.getName());
//            }
//        });
        SingleOutputStreamOperator<cat> cats_filter = cats.filter(value -> "coco".equals(value.getName()));
        cats_filter.print();
        env.execute();


    }
}
