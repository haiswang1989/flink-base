package com.haiswang.flink.demo.datatype;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Java的8大基础类型 + String
 * 
 * Boolean，Byte，Character，Short，Integer，Double，Long，Float
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 下午3:33:31
 */
public class DataTypePrimitiveType {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Integer> dsInt = env.fromElements(1, 2, 3, 4);
        dsInt.print().setParallelism(1);
        
        DataStream<Character> dsChar = env.fromElements('A', 'B', 'C');
        dsChar.print().setParallelism(1);
        
        DataStream<Double> dsDouble = env.fromElements(1.0, 2.0, 3.0);
        dsDouble.print().setParallelism(1);
        
        DataStream<String> dsString = env.fromElements("wang", "hai", "sheng");
        dsString.print().setParallelism(1);
        
        try {
            env.execute("Java Primitive Type");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
