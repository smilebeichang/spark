package cn.sysu.timer;

import cn.sysu.source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author : song bei chang
 * @create 2021/11/23 0:30
 */
public class Flink01_Processing_Time_Timer {

    public static void main(String[] args) throws Exception {

        // 设置端口  可选
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf).setParallelism(1);
        System.out.println(env.getConfig());


        SingleOutputStreamOperator<WaterSensor> stream = env
                // 在socket终端只输入毫秒级别的时间戳
                .socketTextStream("ecs2", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                });


        stream
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 处理时间过后5s后触发定时器
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                        out.collect(value.toString());
                    }

                    // 定时器被触发之后, 回调这个方法
                    // 参数1: 触发器被触发的时间
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println(timestamp);
                        out.collect("我被触发了....");
                    }
                })
                .print();


        env.execute();

    }
}



