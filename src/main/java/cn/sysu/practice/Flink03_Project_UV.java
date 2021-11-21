package cn.sysu.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author : song bei chang
 * @create 2021/11/21 9:57
 */
public class Flink03_Project_UV {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String line, Collector<UserBehavior> out) throws Exception {
                        String[] split = line.split(",");
                        UserBehavior behavior = new UserBehavior(
                                Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                Integer.valueOf(split[2]),
                                split[3],
                                Long.valueOf(split[4]));
                        if ("pv".equals(behavior.getBehavior())) {
                            out.collect(behavior);
                        }
                    }
                })
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    HashSet<Long> userIds = new HashSet<>();

                    @Override
                    public void processElement(UserBehavior userBehavior, Context ctx, Collector<Long> out) throws Exception {
                        // 很lower
//                        int pre = userIds.size();
//                        userIds.add(userBehavior.getUserId());
//                        int post = userIds.size();
//                        // 实现去重(无效访问部分)
//                        if (post - pre == 1) {
//                            out.collect((long) userIds.size());
//                        }
                        // 高级版本
                        if (userIds.add(userBehavior.getUserId())) {
                            out.collect((long) userIds.size());
                        }
                    }
                })
                .print("uv");

        env.execute();

    }


}



