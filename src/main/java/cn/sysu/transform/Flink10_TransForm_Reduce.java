package cn.sysu.transform;

import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author : song bei chang
 * @create 2021/4/19 0:19
 */
public class Flink10_TransForm_Reduce {



    public static void main(String[] args) throws Exception {

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

//        anonymous(waterSensors);
        lambda(waterSensors);
    }

    public static void anonymous(ArrayList<WaterSensor> waterSensors) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<WaterSensor, String> kbSteam = env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);
        kbSteam.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reduce function ...");
                return new WaterSensor(value1.getId(),value1.getTs(),value1.getVc()+value2.getVc());
            }
        })
                .print("reduce");
        env.execute();

    }

    public static void lambda(ArrayList<WaterSensor> waterSensors) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<WaterSensor, String> kbSteam = env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbSteam.reduce((value1, value2) -> {
            System.out.println("reducer function ...");
            return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
        })
                .print("reduce...");

        env.execute();

    }
}



