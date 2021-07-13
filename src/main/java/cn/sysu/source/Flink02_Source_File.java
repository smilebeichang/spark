package cn.sysu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Path;

/**
 * @Author : song bei chang
 * @create 2021/4/18 21:50
 */
public class Flink02_Source_File {
    static  String  absoult_path = "E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Flink\\input\\words.txt";

    static String relative_path = "input";

    //hdfs 执行无法识别
    static String hdfs_path = "hdfs://mycluster/input";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile(relative_path);
        dataStreamSource.print();
        env.execute();
    }
}



