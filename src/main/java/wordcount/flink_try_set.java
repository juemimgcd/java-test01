package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class flink_try_set {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
        DataSource<String> line = env.readTextFile("E:\\java_play\\flink\\src\\main\\java\\wordcount\\word.txt");

        // 3. 转换数据格式
        FlatMapOperator<String, Tuple2<String, Integer>> word_oneDS = line.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> word_one = Tuple2.of(word, 1);
                    collector.collect(word_one);
                }
            }
        });

        // 4. 按照 word 进行分组
        UnsortedGrouping<Tuple2<String, Integer>> word_one_groupby = word_oneDS.groupBy(0);
        // 5. 分组内聚合统计
        AggregateOperator<Tuple2<String, Integer>> sum = word_one_groupby.sum(1);


        // 6. 打印结果
        sum.print();




    }













}
