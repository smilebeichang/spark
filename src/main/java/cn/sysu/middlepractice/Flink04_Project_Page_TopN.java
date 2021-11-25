package cn.sysu.middlepractice;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.TreeSet;

/**
 * @Author : song bei chang
 * @create 2021/11/21 13:28
 *
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 *
 */
public class Flink04_Project_Page_TopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建WatermarkStrategy
        WatermarkStrategy<ApacheLog> wms = WatermarkStrategy
                .<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });

        env
                .readTextFile("input/apache.log")
                .map(line -> {
                    String[] data = line.split(" ");
                    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    return new ApacheLog(data[0],
                            df.parse(data[3]).getTime(),
                            data[5],
                            data[6]);
                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(ApacheLog::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<ApacheLog, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(ApacheLog value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                // <url, count, endWindow>
                }, new ProcessWindowFunction<Long, PageCount, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Long> elements,
                                        Collector<PageCount> out) throws Exception {
                        out.collect(new PageCount(key, elements.iterator().next(), context.window().getEnd()));
                    }
                })
                .keyBy(PageCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, PageCount, String>() {

                    private ValueState<Long> timerTs;
                    private ListState<PageCount> pageState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pageState = getRuntimeContext().getListState(new ListStateDescriptor<PageCount>("pageState", PageCount.class));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                    }

                    @Override
                    public void processElement(PageCount value, Context ctx, Collector<String> out) throws Exception {
                        pageState.add(value);
                        if (timerTs.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                            timerTs.update(value.getWindowEnd());
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 换个排序的思路: 使用TreeSet的自动排序功能
                        TreeSet<PageCount> pageCounts = new TreeSet<>((o1, o2) -> {
                            if (o1.getCount() < o2.getCount()) {
                                return 1;
                            }
                            //  else if(o1.getCount() - o2.getCount() == 0) return 0;
                            else {
                                return -1;
                            }
                        });
                        for (PageCount pageCount : pageState.get()) {
                            pageCounts.add(pageCount);
                            if (pageCounts.size() > 3) { // 如果长度超过N, 则删除最后一个, 让长度始终保持N
                                pageCounts.pollLast();
                            }
                        }
                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: " + (timestamp - 10) + "\n");
                        sb.append("---------------------------------\n");
                        for (PageCount pageCount : pageCounts) {
                            sb.append(pageCount + "\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }

                })
                .print();

        env.execute();
    }
}



