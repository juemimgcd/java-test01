package review;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class flink_tofile {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> elem_DS = env.fromElements(11, 22, 33, 44);
        FileSink<Integer> file = FileSink.<Integer>forRowFormat(new Path("output\\"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("data")
                                .withPartSuffix("txt")
                                .build()
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(new MemorySize(1024))
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .build()
                )
                .build();
        elem_DS.sinkTo(file);


    }
}
