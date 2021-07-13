package cn.sysu.practice;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/4/19 20:09
 */
public class Flink01_Project_PV {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String regex = ",";
                    String[] split = line.split(regex);
                    return new UserBehavior(Long.valueOf(split[0]),Long.valueOf(split[1]),Integer.valueOf(split[2]),split[3],Long.valueOf(split[4]));
                })
                //过滤出pv行为
                .filter(behavior -> "pv".equals(behavior.getBehavior()))
                //使用tuple类型，方便后面求和
                .map(behavior -> Tuple2.of("pv",1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                //按照key分组
                .keyBy(value -> value.f0)
                //求和
                .sum(1)
                .print();

        env.execute();

    }
}



