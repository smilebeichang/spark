package cn.sysu.state;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : song bei chang
 * @create 2021/11/23 1:03
 */
public class Flink01_State_Operator_List {

    public static void main(String[] args) throws Exception {

        // 设置端口  可选
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf)
                .setParallelism(2);

        // 每秒做一次checkpoint  mode: "exactly once" , "at least once" guaranteed.
        env.enableCheckpointing(1000);

        env
                .socketTextStream("ecs2", 9999)
                .map(new MyCountMapper())
                .print();

        env.execute();

    }

    /**
     * 使用状态需要实现 CheckpointedFunction
     */
    private static class MyCountMapper implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            count++;

            // 抛出异常，验证 flink 是否自动重启 不停的输入a,就一定可以看到webUI界面的重启
            if (value.contains("a")){
                throw new RuntimeException();
            }

            return count;


        }




        // 恢复状态 初始化时会调用这个方法，向本地状态中填充数据. 每个子任务调用一次 由并行度绝对
        // list 和 unionList 的区别只在于调用和恢复  .getListState()  均匀分配 vs 合并为全量State 再分发给每个实例
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            state = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state", Long.class));
            for (Long c : state.get()) {
                count += c;
            }
        }

        // 本地状态持久化 Checkpoint时会调用这个方法  由并行度绝对
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println(" 周期性执行 snapshotState...");
            // state 来自全局变量
            state.clear();
            //追加 (追加和覆盖针对的都是同一个集合)
            //state.add(count);
            //覆盖
            //wordList.update(words);
        }

    }
}



