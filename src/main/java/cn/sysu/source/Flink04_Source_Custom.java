package cn.sysu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/4/18 22:29
 */
public class Flink04_Source_Custom {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> dataStreamSource = env.addSource(new MySource("hadoop162", 9999));

        dataStreamSource.print();
        env.execute();


    }
}



