package cn.sysu.table_sql;

import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_ROW;

/**
 * @Author : song bei chang
 * @create 2021/11/25 22:37
 */
public class Flink16_HiveCatalog {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Catalog 名字
        String name            = "myhive";
        // 默认数据库
        String defaultDatabase = "default";
        // hive配置文件的目录. 需要把hive-site.xml添加到该目录
        String hiveConfDir     = "src\\main\\resources";

        // 1. 创建HiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        // 2. 注册HiveCatalog
        tEnv.registerCatalog(name, hive);
        // 3. 把 HiveCatalog: myhive 作为当前session的catalog
        tEnv.useCatalog(name);
        tEnv.useDatabase("default");
        tEnv.sqlQuery(" select * from student ").execute().print();
    }

}



