package cn.sysu.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/4/18 23:49
 */
public class Flink06_TransForm_Shuffle {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5)
                //random.nextInt(numberOfChannels)
                .shuffle()
                .print();
        env.execute();
    }
}



