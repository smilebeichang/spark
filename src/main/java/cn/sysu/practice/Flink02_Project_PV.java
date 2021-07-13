package cn.sysu.practice;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author : song bei chang
 * @create 2021/4/19 20:24
 */
public class Flink02_Project_PV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String [] spilt = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(spilt[0]),
                            Long.valueOf(spilt[1]),
                            Integer.valueOf(spilt[2]),
                            spilt[3],
                            Long.valueOf(spilt[4])
                    );
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior()))
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    long count = 0;

                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                })
                .print();

        env.execute();

    }
}



