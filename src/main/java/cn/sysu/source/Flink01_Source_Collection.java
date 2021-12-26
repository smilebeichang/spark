package cn.sysu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/11/10 21:37
 */
public class Flink01_Source_Collection {


    public static void main(String[] args) throws Exception {

        // 默认并行度本地电脑的cpu核数  5
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 15777844001L, 45),
                new WaterSensor("ws_002", 15777844015L, 43),
                new WaterSensor("ws_003", 15777844020L, 42)
        );

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> dataStreamSource = env.fromCollection(waterSensors);
        dataStreamSource.print("ds 打印:");
        env.execute();

    }



}



