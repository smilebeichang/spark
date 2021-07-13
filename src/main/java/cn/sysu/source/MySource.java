package cn.sysu.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author : song bei chang
 * @create 2021/4/18 22:31
 */
public class MySource implements SourceFunction<WaterSensor> {

    private String host;
    private int port;
    private volatile boolean isRunning = true;
    private Socket socket;

    public MySource(String host, int post) {
        this.host = host;
        this.port = post;
    }

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        socket = new Socket(host,port);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        String line = null;
        while ( isRunning && (line  = reader.readLine()) != null) {
            String[] split = line.split(",");
            ctx.collect(new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2])));

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}



