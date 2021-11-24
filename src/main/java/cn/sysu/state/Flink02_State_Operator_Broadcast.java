package cn.sysu.state;


import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author : song bei chang
 * @create 2021/11/23 1:06
 */
public class Flink02_State_Operator_Broadcast {

    public static void main(String[] args) throws Exception {
        // 设置端口  可选
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf)
                .setParallelism(3);

        // 获取两个流
        DataStreamSource<String> dataStream = env.socketTextStream("ecs2", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("ecs2", 8888);


        // 将控制流转换为广播流的
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);

        // 广播流
        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor);

        dataStream
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 从广播状态中取值, 不同的值做不同的业务
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        if ("1".equals(state.get("switch"))) {
                            out.collect("切换到1号配置....");
                        } else if ("0".equals(state.get("switch"))) {
                            out.collect("切换到0号配置....");
                        } else {
                            out.collect("切换到其他配置....");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        // 把值放入广播状态
                        state.put("switch", value);
                    }
                })
                .print();

        env.execute();
    }

}



