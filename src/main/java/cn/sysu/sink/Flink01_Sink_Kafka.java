package cn.sysu.sink;


import cn.sysu.source.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;

/**
 * @Author : song bei chang
 * @create 2021/11/21 0:44
 */
public class Flink01_Sink_Kafka {

    public static void main(String[] args) throws Exception {

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .fromCollection(waterSensors)
                .map(JSON::toJSONString)
                .addSink(new FlinkKafkaProducer<String>("ecs2:9092", "topic_sensor", new SimpleStringSchema()));


        env.execute();
    }
}



