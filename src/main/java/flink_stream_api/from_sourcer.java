package flink_stream_api;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class from_sourcer {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> file_source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                new Path("E:\\java_play\\flink\\src\\main\\java\\wordcount\\word.txt")).build();



        env.fromSource(file_source, WatermarkStrategy.noWatermarks(),"file_source")
            .print();


        env.execute();








    }
}
