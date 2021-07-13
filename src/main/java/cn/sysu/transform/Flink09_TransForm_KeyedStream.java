package cn.sysu.transform;

import cn.sysu.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author : song bei chang
 * @create 2021/4/19 0:03
 */
public class Flink09_TransForm_KeyedStream {

    public static void main(String[] args) throws Exception {
//        kb();
//        pojo();
        maxBy();
    }

    public static void kb() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 4, 5, 3);
        KeyedStream<Integer, String> kb = stream.keyBy(ele -> ele % 2 == 0 ? "偶数" : "奇数");

        kb.sum(0).print("sum");
        kb.max(0).print("max");
        kb.min(0).print("min");

        env.execute();
    }

    public static void pojo() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream
                .sum("vc")
                .print("maxBy...");

        env.execute();
    }


    public static void maxBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream
                .maxBy("vc", true)
                .print("max...");

        env.execute();

    }
}



