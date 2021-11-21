package cn.sysu.sideoutput;


import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


/**
 * @Author : song bei chang
 * @create 2021/11/21 12:30
 */
public class Flink01_SideOutput_spilt {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        System.out.println(env.getConfig());


        SingleOutputStreamOperator<WaterSensor> result =
                env
                        // 在socket终端只输入毫秒级别的时间戳
                        .socketTextStream("ecs2", 9999)
                        .map(new MapFunction<String, WaterSensor>() {
                            @Override
                            public WaterSensor map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                            }
                        })
                        .keyBy(ws -> ws.getTs())
                        .process(new KeyedProcessFunction<Long, WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                out.collect(value);
                                //水位大于5的写入到侧输出流
                                if (value.getVc() > 5) {
                                    ctx.output(new OutputTag<WaterSensor>("警告") {
                                    }, value);
                                }
                            }
                        });
        result.print();
        result.getSideOutput(new OutputTag<WaterSensor>("side_1") {
        }).print();
        env.execute();

    }

}



