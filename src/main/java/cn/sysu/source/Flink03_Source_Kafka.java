package cn.sysu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author : song bei chang
 * @create 2021/11/10 22:11
 */
public class Flink03_Source_Kafka {

    public static void main(String[] args) throws Exception {

        // docker
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id","Flink03_Source_Kafka");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // ecs  正常使用 公网IP 域名均正常
        Properties properties2 = new Properties();
        //properties2.setProperty("bootstrap.servers","47.119.157.0:9092,47.119.169.212:9092,8.135.102.7:9092");
        properties2.setProperty("bootstrap.servers","ecs2:9092,ecs3:9092,ecs4:9092");
        properties2.setProperty("group.id","Flink03_Source_Kafka");
        properties2.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");



        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 反序列化器 SimpleStringSchema JSONKeyValueDeserializationSchema
        env.addSource(new FlinkKafkaConsumer<>("first",new SimpleStringSchema(),properties2)).print();



        env.execute();
    }

}



