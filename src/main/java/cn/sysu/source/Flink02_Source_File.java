package cn.sysu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Path;

/**
 * @Author : song bei chang
 * @create 2021/11/10 21:50
 */
public class Flink02_Source_File {

    static  String  absoult_path = "E:\\BaiduNetdiskDownload\\02-hadhoop\\01-Maven\\Flink\\input\\words.txt";

    static String relative_path = "input";

    // hdfs 执行无法识别  flink默认未添加hadoop的相关jar,添加依赖即可
    static String hdfs_path = "hdfs://ecs2:9820/flink/input/wordcount.txt";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile(relative_path);
        dataStreamSource.print("readTextFile:");
        env.execute();

    }
}



