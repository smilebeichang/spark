package cn.sysu.sink;

import cn.sysu.source.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @Author : song bei chang
 * @create 2021/11/21  0:46
 */
public class Flink04_Sink_Custom {

    public static void main(String[] args) throws Exception {

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.fromCollection(waterSensors)
                .addSink(new RichSinkFunction<WaterSensor>() {

                    private PreparedStatement ps;
                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://ecs2:3306/hive?useSSL=false", "root", "sbc006688");
                        ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");
                    }

                    @Override
                    public void close() throws Exception {
                        ps.close();
                        conn.close();
                    }

                    @Override
                    public void invoke(WaterSensor value, Context context) throws Exception {
                        ps.setString(1, value.getId());
                        ps.setLong(2, value.getTs());
                        ps.setInt(3, value.getVc());
                        ps.execute();
                    }
                });


        env.execute();
    }

}



