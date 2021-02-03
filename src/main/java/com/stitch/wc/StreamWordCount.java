package com.stitch.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        //String inputPath = "D:\\code\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        //DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 流式数据源
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于流式数据处理
        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap( new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountDataStream.print().setParallelism(3);
        env.execute();
    }

}
