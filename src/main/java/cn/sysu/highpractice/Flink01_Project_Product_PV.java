package cn.sysu.highpractice;


import cn.sysu.practice.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author : song bei chang
 * @create 2021/11/21 13:10
 * <p>
 * 设置滚动时间窗口，实时统计每小时内的网站PV。此前我们已经完成了该需求的流数据操作，当前需求是在之前的基础上增加了窗口信息。
 */
public class Flink01_Project_Product_PV {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        env
                .readTextFile("input/UserBehavior.csv")
                // 对数据切割, 然后封装到POJO中
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])
                    );
                })
                //过滤出pv行为
                .filter(behavior -> "pv".equals(behavior.getBehavior()))
                // 添加 Watermark
                .assignTimestampsAndWatermarks(wms)
                .map(behavior -> Tuple2.of("pv", 1L))
                // 使用Tuple类型, 方便后面求和
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                // keyBy: 按照key分组
                .keyBy(value -> value.f0)
                // 分配窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                // 求和
                .sum(1)
                .print();
        env.execute();
    }
}



