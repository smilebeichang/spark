package cn.sysu.table_sql;


import cn.sysu.source.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// 静态导入(可以把静态类方法成员变量均导进来) vs 动态导入
import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author : song bei chang
 * @create 2021/11/21 13:59
 */
public class Flink01_TableApi_BasicUse {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60)
                );

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 将流转换成动态表. 表的字段名从pojo的属性名自动抽取   (将一维的流转化为二维的表)(还可以指定scheme 修改字段名和类型)
        Table table = tableEnv.fromDataStream(waterSensorStream);

        // 3. 对动态表进行查询  引入 静态方法类 Expressions
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        // 4. 把动态表转换成流(toAppendStream 查询的结果只有新增,没有更新和删除的时候使用)(toRetractStream 适用于更新的时候,如聚合)  Row.class
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



