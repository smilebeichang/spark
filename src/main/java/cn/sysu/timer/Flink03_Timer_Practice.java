package cn.sysu.timer;

import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author : song bei chang
 * @create 2021/11/23 0:30
 *
 * 需求：
 *  监控水位传感器的水位值，如果水位值在5s之内(event time)连续上升，则报警。
 *  滚动窗口无法重叠、滑动窗口无法确定启动时间和频率，故采用定时器
 *  难点：时间和水位的更新、定时器的创建和删除
 *
 */
public class Flink03_Timer_Practice {

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


        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000);


        stream
                .assignTimestampsAndWatermarks(wms)
                // 匿名内部类---Lambda表达式---方法引用
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    int lastVc = 0;
                    long timerTS = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVc) {
                            if (timerTS == Long.MIN_VALUE) {
                                System.out.println("注册....");
                                timerTS = ctx.timestamp() + 5000L;
                                ctx.timerService().registerEventTimeTimer(timerTS);
                            }
                        } else {
                            ctx.timerService().deleteEventTimeTimer(timerTS);
                            timerTS = Long.MIN_VALUE;
                        }

                        lastVc = value.getVc();
                        System.out.println(lastVc);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 报警!!!!");
                        timerTS = Long.MIN_VALUE;
                    }
                })
                .print();


        env.execute();

    }
}



