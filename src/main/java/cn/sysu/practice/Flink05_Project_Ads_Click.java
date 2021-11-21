package cn.sysu.practice;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.apache.flink.api.common.typeinfo.Types.TUPLE;

/**
 * @Author : song bei chang
 * @create 2021/11/21 11:20
 */
public class Flink05_Project_Ads_Click {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/adClickLog.csv")
                .map(line -> {
                    String regex = ",";
                    String[] split = line.split(regex);
                    return new AdsClickLog(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            split[2],
                            split[3],
                            Long.valueOf(split[4])

                    );
                })
                .map(log -> Tuple2.of(Tuple2.of(log.getProvince(),log.getAdId()),1L))
                .returns(TUPLE(TUPLE(STRING,LONG),LONG))
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print("省份-广告");
        env.execute();

    }
}



