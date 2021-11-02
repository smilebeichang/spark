package cn.sysu.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/7/14 10:29
 */
public class Flink05_Stream_Parallelism {

    /**
        如何控制操作链:
        1. .startNewChain()
            从当前算子开启一个新链
        2. .disableChaining()
            当前算子不会和任何的算子组成链
        3.env.disableOperatorChaining();
            全局禁用操作链

        给算子设置并行度:
        1. 在配置文件中 flink.yaml
                   parallelism.default: 1
        2.1 在提交job的时候通过参数传递
            -p 3
        2.2 通过执行环境来设置并行度
            env.setParallelism(1);
        3. 单独的给每个算子设置并行度
     */

    public static void main(String[] args) throws Exception {
        System.out.println("main.....");
        //xxxx
        // 1. 获取流的执行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
        // 设置所有的算子不形成任务链
        //env.disableOperatorChaining();
       
        // 2. 从数据源获取一个流
        // 3. 对流做各种转换
        SingleOutputStreamOperator<Tuple2<String, Long>> result = env
            .socketTextStream("hadoop162", 9999)
            .flatMap((String line, Collector<String> out) -> {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            })
            .returns(Types.STRING)
            //            .returns(String.class)
            .map(word -> Tuple2.of(word, 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(t -> t.f0)
        
            .sum(1);
        
        // 4. 输出结果
        result.print();
        
        // 5. 启动执行环境
        env.execute();
    }
}
/*
如何控制操作链:

1. .startNewChain()
    从当前算子开启一个新链
    
2. .disableChaining()
    当前算子不会和任何的算子组成链
    
3.env.disableOperatorChaining();
    全局禁用操作链



给算子设置并行度:
1. 在配置文件中 flink.yaml
    parallelism.default: 1
    
2. 在提交job的时候通过参数传递
    -p 3

3. 通过执行环境来设置并行度
    env.setParallelism(1);
    
4. 单独的给每个算子设置并行度
 




 */