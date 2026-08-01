package flink_stream_api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;
import java.util.Arrays;

public class from_collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> collect = env.fromCollection(Arrays.asList(1,2,3));
        collect.print();

        ArrayList<cat> collect1 = new ArrayList<cat>();
        collect1.add(new cat(19,"coco"));
        DataStreamSource<cat> collect2 = env.fromCollection(collect1);
        collect2.print();



        env.execute();
    }
}
