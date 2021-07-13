package cn.sysu.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/4/18 23:52
 */
public class Flink07_TransForm_Connnect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c");

        //将流连接在一起  貌合神离
        ConnectedStreams<Integer, String> cs = integerDataStreamSource.connect(stringDataStreamSource);
        cs.getFirstInput().print("first");
        cs.getSecondInput().print("second");
        env.execute();


    }
}



