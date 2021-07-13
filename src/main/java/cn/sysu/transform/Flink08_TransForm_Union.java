package cn.sysu.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/4/18 23:57
 */
public class Flink08_TransForm_Union {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 4, 5, 3);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 40, 50, 30);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 400, 500, 300);
        DataStreamSource<Integer> stream4 = env.fromElements(1000, 2000, 4000, 5000, 3000);

        stream1.union(stream2)
                .union(stream3)
                .union(stream4)
                .print("allStream");
        env.execute();
    }
}



