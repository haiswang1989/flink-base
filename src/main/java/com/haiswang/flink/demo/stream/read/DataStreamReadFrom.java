package com.haiswang.flink.demo.stream.read;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream 读
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月16日 下午3:47:32
 */
public class DataStreamReadFrom {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> lines = env.readTextFile("input.txt"); //一行一行的读取文件
        lines.print().setParallelism(1);
        
        try {
            env.execute("Data Stream Read Type");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
