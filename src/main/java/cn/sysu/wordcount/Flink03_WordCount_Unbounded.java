package cn.sysu.wordcount;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author : song bei chang
 * @create 2021/4/17 23:40
 */
public class Flink03_WordCount_Unbounded {

    /*
        lambda  拉姆达
        java提供的一种函数式编程
        当一个方法, 接收一个接口, 并且这个接口只有一个抽象方法, 就可以使用lambda表达式了
        () -> 实现
     */

    public static void main(String[] args) throws Exception {


        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件  nc hadoop102 44444  Caused by: java.net.ConnectException: Connection timed out: connect
        /*
            临时用nc开放测试端口
            1. 开启测试端口
                nc -tl 2333 &
            2. 查看测试端口
                netstat -an | grep 2333
            3. 关闭测试端口
                ps -ef | grep -v grep | grep nc
         */
        //DataStreamSource<String> lineDSS = env.socketTextStream("ecs2", 8888);
        DataStreamSource<String> lineDSS = env.socketTextStream("47.119.157.0", 8888);

        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                .keyBy(t -> t.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                .sum(1);

        // 6. 打印
        result.print();

        // 7. 执行
        env.execute();

    }
}



