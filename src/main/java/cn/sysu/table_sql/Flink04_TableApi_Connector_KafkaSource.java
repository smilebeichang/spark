package cn.sysu.table_sql;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author : song bei chang
 * @create 2021/11/25 00:59
 *
 * 前面是先得到流, 再转成动态表, 其实动态表也可以直接连接到数据
 * 通过connect连接kafka
 */
public class Flink04_TableApi_Connector_KafkaSource {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 2.  创建表
        // 2.1 表的元数据信息
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        // 2.2 连接kafka, 并创建一个临时表, 其实就是一个动态表
        tableEnv
                .connect(new Kafka()
                        // 最新的版本已经不需要了，表示通用型，不需要版本号
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "beichang")
                        .property("bootstrap.servers", "ecs2:9092,ecs3:9092,ecs4:9092"))
                // s1,1,1
                //.withFormat(new Csv())
                // {"id":"s1","ts":1,"vc":1}
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 3. 对动态表进行查询
        Table sensorTable = tableEnv.from("sensor");

        sensorTable.execute().print();


        Table resultTable = sensorTable
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        // 4. 把动态表转换成流. 如果涉及到数据的更新, 要用到撤回流. 多个了一个boolean标记
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);

        // 5. 打印方式 1.转换成流再打印  2.直接执行一下变成TableResult打印
        resultStream.print();
        resultTable.execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



