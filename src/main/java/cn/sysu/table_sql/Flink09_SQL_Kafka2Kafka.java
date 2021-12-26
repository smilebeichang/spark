package cn.sysu.table_sql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/25 1:15
 *
 * kafka 只能新增不能有撤回流和更新等操作
 */
public class Flink09_SQL_Kafka2Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 注册SourceTable: source_sensor
        tableEnv.executeSql(
                "create table source_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source_sensor',"
                + "'properties.bootstrap.servers' = 'ecs2:9029,ecs3:9092,ecs4:9092',"
                + "'properties.group.id' = 'bei_chang',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json'"
                + ")");

        // 2. 注册SinkTable: sink_sensor
        tableEnv.executeSql(
                "create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'ecs2:9029,ecs3:9092,ecs4:9092',"
                + "'format' = 'json'"
                + ")");

        // 3. 从SourceTable 查询数据, 并写入到 SinkTable
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");
    }
}


