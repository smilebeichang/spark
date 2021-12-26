package cn.sysu.sink;

import cn.sysu.practice.MarketingUserBehavior;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @Author : song bei chang
 * @create 2021/12/25 19:06
 */
public class Flink05_Sink_KafkaV2  {

    public static void main(String[] args) throws Exception {

        saveToKafka();

    }

    private static void saveToKafka() throws Exception {


        // ecs  公网IP 域名均正常
        Properties properties2 = new Properties();
        properties2.setProperty("bootstrap.servers","ecs2:9092,ecs3:9092,ecs4:9092");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        env
                .addSource(new KafkaDataSource())
                .map(JSON::toJSONString)
                .addSink(
                        new FlinkKafkaProducer<>("topic_sensor", new SimpleStringSchema(),properties2)
                );

        env.execute();

    }



    private static class KafkaDataSource extends RichSourceFunction<MarketingUserBehavior> {

        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );
                ctx.collect(marketingUserBehavior);
                System.out.println(marketingUserBehavior);
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }


}



