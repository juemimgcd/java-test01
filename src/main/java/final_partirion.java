import flink_partition.partition_key;
import flink_to_out.Cat;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class final_partirion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        env.fromElements(new Cat(2,"didi",22),
                new Cat(3,"lily",44),
                new Cat(2,"lil",47),
                new Cat(5,"ily",64),
                new Cat(7,"liy",34),
                new Cat(4,"ly",45))

                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }
                }, new KeySelector<Cat, Integer>() {
                    @Override
                    public Integer getKey(Cat value) throws Exception {
                        return value.getAge();
                    }
                })
                .print().setParallelism(4);
        env.execute();








    }



}
