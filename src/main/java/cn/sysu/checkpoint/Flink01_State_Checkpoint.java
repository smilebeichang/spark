package cn.sysu.checkpoint;


import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author : song bei chang
 * @create 2021/11/24 0:00
 *
 * Kafka+Flink+Kafka 实现端到端严格一次
 *  source —— kafka consumer作为source，可以将偏移量保存下来，如果后续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性
 *  内部 —— 利用flink checkpoint机制，把状态存盘，发生故障的时候可以恢复，保证部的状态一致性
 *  sink —— kafka producer作为sink，采用两阶段提交，需要实现一个 TwoPhaseCommitSinkFunction
 *
 */
public class Flink01_State_Checkpoint {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Properties sourProps = new Properties();
        sourProps.setProperty("bootstrap.servers", "ecs2:9092,ecs3:9092,ecs4:9092");
        sourProps.setProperty("group.id", "Flink01_State_Checkpoint");
        sourProps.setProperty("auto.offset.reset", "latest");

        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "ecs2:9092,ecs3:9092,ecs4:9092");
        // The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
        sinkProps.setProperty("transaction.timeout.ms", 14 * 60 * 1000 + "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                // 默认端口8081
                .createLocalEnvironmentWithWebUI(new Configuration())
                .setParallelism(3);

        env.setStateBackend(new RocksDBStateBackend("hdfs://ecs2:9820/flink/checkpoints/rocksdb"));
        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);

        // 高级选项：
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env
                .addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), sourProps))
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(
                            datas[0],
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2])
                    );

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));

                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                        Integer lastVc = state.value() == null ? 0 : state.value();

                        if (Math.abs(value.getVc() - lastVc) >= 10) {
                            out.collect(value.getId() + " 红色警报!!!");
                        }

                        state.update(value.getVc());
                    }
                })
                // FlinkKafkaProducer.  默认 Semantic.AT_LEAST_ONCE,
                // .addSink(new FlinkKafkaProducer<String>("ecs2:9092", "alert", new SimpleStringSchema()));

                // 修改为 Semantic.EXACTLY_ONCE
                .addSink(new FlinkKafkaProducer<String>(
                            "default",
                            new KafkaSerializationSchema<String>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                                return new ProducerRecord<>("alter",element.getBytes(StandardCharsets.UTF_8) );
                            }
                        },sinkProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                        )
                );

        env.execute();
    }

}



