package cn.sysu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/10 22:29
 */
public class Flink04_Source_Custom {


    /**
     * 1.监听ecs2的9999端口
     * 2.启动main
     * 3.输入以下参数 spilt
     *     sensor_1,1607527992000,20
     *     sensor_2,1607527993000,40
     *
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> dataStreamSource = env.addSource(new MySource("ecs2", 9999));

        dataStreamSource.print();
        env.execute();


    }
}



