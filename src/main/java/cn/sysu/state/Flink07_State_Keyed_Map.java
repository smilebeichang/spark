package cn.sysu.state;


import cn.sysu.source.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author : song bei chang
 * @create 2021/11/23 23:06
 *
 * 去重: 去掉重复的水位值.
 *    思路: 把水位值作为MapState的key来实现去重, value随意
 */
public class Flink07_State_Keyed_Map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);

        env
                .socketTextStream("ecs2", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private MapState<Integer, String> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = this
                                .getRuntimeContext()
                                .getMapState(new MapStateDescriptor<Integer, String>("mapState", Integer.class, String.class));
                    }
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (!mapState.contains(value.getVc())) {
                            out.collect(value);
                            mapState.put(value.getVc(), "随意value");
                        }
                    }
                })
                .print();
        
        env.execute();
    }
}


