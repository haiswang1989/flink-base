package com.haiswang.flink.demo.datatype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据类型之  Java Tuple{n}
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 上午11:28:28
 */
public class DataTypeTuples {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Tuple2<Integer, String>> familyInfos = env.fromElements(
                                                            new Tuple2<Integer, String>(5, "wanghaisheng"), 
                                                            new Tuple2<Integer, String>(4, "wanghaiming"));
        
        familyInfos
            .map((Tuple2<Integer, String> value) -> value.f1)
            .print()
            .setParallelism(1);
        
        try {
            env.execute("Flink datatype tuple.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
