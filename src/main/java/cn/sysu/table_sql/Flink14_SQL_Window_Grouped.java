package cn.sysu.table_sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author song bei chang
 * @Date 2021/12/03 23:03
 */
public class Flink14_SQL_Window_Grouped {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));
        
        
        tenv.executeSql("create table sensor(" +
                            "   id string, " +
                            "   ts  bigint," +
                            "   vc int, " +
                            "   et as to_timestamp(from_unixtime(ts/1000)), " +
                            "   watermark for et as et - interval  '3' second" +
                            ")with(" +
                            "   'connector' = 'filesystem', " +
                            "   'path' = 'input/sensor.txt'," +
                            "   'format' = 'csv'" +
                            ")");
        
        // sql语句中使用分组窗口
        
        // 滚动
        /*tenv.sqlQuery("select" +
                          " id," +
                          " tumble_start(et, interval '5' second) w_start," +
                          " tumble_end(et, interval '5' second) w_end," +
                          " sum(vc) sum_vc " +
                          " from sensor " +
                          "group by id, tumble(et, interval '5' second)")
            .execute()
            .print();*/

        // 滑动
        tenv.sqlQuery("select" +
                          " id," +
                          " hop_start(et, interval '2' second, interval '4' second) w_start," +
                          " hop_end(et, interval '2' second, interval '4' second) w_end," +
                          " sum(vc) sum_vc " +
                          " from sensor " +
                          "group by id, hop(et, interval '2' second, interval '4' second)")
            .execute()
            .print();

        // 会话
       /* tenv.sqlQuery("select" +
                          " id," +
                          " session_start(et, interval '2' second) w_start," +
                          " session_end(et, interval '2' second) w_end," +
                          " sum(vc) sum_vc " +
                          " from sensor " +
                          "group by id, session(et, interval '2' second)")
            .execute()
            .print();*/
        
        
        
    }
}
