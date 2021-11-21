package cn.sysu.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author : song bei chang
 * @create 2021/11/21 11:42
 */
public class Flink05_CountWindow {


    /**
     * 基于元素个数的窗口
     * 窗口分配器  全局窗口
     *
     * 时间间隔可以通过: Time.milliseconds(x), Time.seconds(x), Time.minutes(x)等来指定
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("ecs2", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split("\\W+")).forEach(word -> out.collect(Tuple2.of(word, 1L)));
                    }
                })
                .keyBy(t -> t.f0)
                // 添加滚动窗口
                .countWindow(3)
                .sum(1)
                .print();

        env.execute();

    }
}



