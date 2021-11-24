package cn.sysu.state;


import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author : song bei chang
 * @create 2021/11/23 22:56
 *
 * 检测传感器的水位值，如果连续的两个水位值超过10，就输出报警。
 *
 */
public class Flink03_State_Keyed_Value {

    public static void main(String[] args) throws Exception {

        // 设置端口  可选
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf)
                .setParallelism(3);

        env
                .socketTextStream("ecs2", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ValueState<Integer> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取运行池  Ctrl + Alt + F 提升为全局变量
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // lastVc 根据状态来赋值  如果第一次,则赋值为0
                        Integer lastVc = state.value() == null ? 0 : state.value();
                        if (Math.abs(value.getVc() - lastVc) >= 10) {
                            out.collect(value.getId() + " 红色警报!!!");
                        }
                        state.update(value.getVc());
                    }
                })
                .print();

        env.execute();
    }
}



