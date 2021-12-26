package cn.sysu.table_sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author : song bei chang
 * @create 2021/11/25 22:13
 */
public class Flink13_TableApi_EventTime_2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 若需要转换为东八区,可使用以下配置
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));

        // 作为事件时间的字段必须是 timestamp 类型, 所以可根据 long 类型的 ts 计算出来一个 t
        // sql 的水印比 table 简单很多
        tEnv.executeSql(
                "create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                // long --> 'yyyy-MM-dd' --> timestamp
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        tEnv.sqlQuery("select * from sensor").execute().print();

    }

}



