package cn.sysu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author : song bei chang
 * @create 2021/4/18 22:11
 */
public class Flink03_Source_Kafka {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","ecs2:9092,ecs3:9092,ecs4:9092");
        properties.setProperty("group.id","Flink03_Source_Kafka");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new FlinkKafkaConsumer<>("sensor",new SimpleStringSchema(),properties)).print();

        env.execute();
    }

}



