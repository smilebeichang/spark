package cn.sysu.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/21 00:23
 */
public class Flink06_TransForm_Shuffle {

    /**
     * random.nextInt(numberOfChannels) 类似
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5)
                .shuffle()
                .print();
        env.execute();

    }

}



