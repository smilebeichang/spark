package cn.sysu.function;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author : song bei chang
 * @create 2021/11/25 23:03
 */
public class Flink01_ToUpperCase{

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> stream = env.fromElements("hello", "xiaopang", "Hello");

        Table table = tEnv.fromDataStream(stream, $("word"));

        // 1. 注册临时函数
        tEnv.createTemporaryFunction("toUpper", ToUpperCase.class);
        // 2. 注册临时表
        tEnv.createTemporaryView("t_word", table);
        // 3. 在临时表上使用自定义函数查询   用法和之前的hive一致
        tEnv.sqlQuery("select toUpper(word) word_upper from t_word").execute().print();

    }


    // 定义一个可以把字符串转成大写标量函数
    public static  class ToUpperCase extends ScalarFunction {
        public String eval(String s){
            return s.toUpperCase();
        }
    }


}





