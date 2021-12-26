package cn.sysu.table_sql;

import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneOffset;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author song bei chang
 * @Date 2021/7/24 10:03
 */
public class Flink15_SQL_OverWindow {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
            .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                          new WaterSensor("sensor_1", 2000L, 20),
                          new WaterSensor("sensor_1", 2000L, 30),
                          new WaterSensor("sensor_1", 3000L, 40),
                          new WaterSensor("sensor_1", 5000L, 50)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
            );
        
        Table table = tenv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime().as("et"), $("vc"));
        tenv.createTemporaryView("sensor", table);

        // 初级版本:开窗
        /*tenv.sqlQuery("select" +
                          " id," +
                          " et," +
                          " vc," +
                          " sum(vc) over(partition by id order by et rows between unbounded preceding and current row) vc_sum, " +
                          " max(vc) over(partition by id order by et rows between unbounded preceding and current row) vc_max, " +
                          " min(vc) over(partition by id order by et rows between unbounded preceding and current row) vc_min " +
//                          " sum(vc) over(partition by id order by et rows between 1 preceding and current row) vc_sum2 " +
//                          " sum(vc) over(partition by id order by et range between unbounded preceding and current row) vc_sum2 " +
//                          " sum(vc) over(partition by id order by et range between interval '1' second preceding and current row) vc_sum2 " +
                          "from sensor ")
            .execute()
            .print();*/

        // 升级版本:将窗口放到后面去,更简洁
        tenv.sqlQuery("select" +
                          " id," +
                          " et," +
                          " vc," +
                          " sum(vc) over w sum_vc, " +
                          " max(vc) over w  vc_max, " +
                          " min(vc) over w vc_min " +
                          "from default_catalog.default_database.sensor " +
                          "window w as (partition by id order by et rows between unbounded preceding and current row)")
            .execute()
            .print();
        
        
    }
}
