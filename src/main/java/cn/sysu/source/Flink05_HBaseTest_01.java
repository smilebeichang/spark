package cn.sysu.source;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


/**
 * @Author : song bei chang
 * @create 2021/12/25 12:21
 */
public class Flink05_HBaseTest_01 {

    public static void main(String[] args) throws Exception {
        // 批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);





        // 创建用户-电影表 u_m
        TableResult tableResult = tableEnv.executeSql(
                "CREATE TABLE u_m (" +
                        " rowkey STRING," +
                        " u_m_r ROW<r STRING>," +
                        " PRIMARY KEY (rowkey) NOT ENFORCED" +
                        " ) WITH (" +
                        " 'connector' = 'hbase-2.2' ," +
                        " 'table-name' = 'default:u_m_01' ," +
                        " 'zookeeper.quorum' = 'ecs2:2181'" +
                        " )");

        // 查询是否能获取到HBase里的数据
//        Table table = tableEnv.sqlQuery("SELECT rowkey, u_m_r FROM u_m");

        // 相当于 scan
        Table table = tableEnv.sqlQuery("SELECT * FROM u_m");


        // 查询的结果
        TableResult executeResult = table.execute();

        // 获取查询结果
        CloseableIterator<Row> collect = executeResult.collect();

        // 输出 (执行print或者下面的 Consumer之后，数据就被消费了。两个只能留下一个)
        //executeResult.print();

        List<UserMovie> userMovieList = new ArrayList<>();

        collect.forEachRemaining(new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                String field0 = String.valueOf(row.getField(0));
                String[] user_movie = field0.split(",");
                Double ratting = Double.valueOf(String.valueOf(row.getField(1)));
                userMovieList.add(new UserMovie(user_movie[0],user_movie[1],ratting));
            }
        });


        System.out.println("................");

        for(UserMovie um : userMovieList){
            System.out.println(um);
        }



    }
}


