package com.stitch.apitest.source;

import com.stitch.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.*;

public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718200L, 15.4),
                new SensorReading("sensor_2", 1547718201L, 16.5),
                new SensorReading("sensor_3", 1547718202L, 6.7),
                new SensorReading("sensor_4", 1547718203L, 37.6)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 67, 189);

        // 打印输出
        dataStream.print("data");
        integerDataStream.print("int");

        // 执行
        env.execute();

    }

}
