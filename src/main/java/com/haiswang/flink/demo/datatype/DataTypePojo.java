package com.haiswang.flink.demo.datatype;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.haiswang.flink.demo.common.WordCount;

/**
 * 数据类型之 POJO
 * 
 * 何为Pojo
 * 1：class必须是Public修饰的
 * 2：必须有无参构造函数
 * 3：所有的成员都是Public的,或者提供了Getter和Setter方法的Private的成员
 * 4：所有的成员类型必须是Flink支持的,Flink使用avro去序列化"随意"的object对象(比如Date)
 * 
 * Flink分析Pojo对象,了解其结构,所以可以更搞笑的处理Pojo对象
 * 
 * POJO对象com.haiswang.flink.demo.common.WordCount
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月15日 上午11:38:03
 */
public class DataTypePojo {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WordCount> wordsCount = 
                env.fromElements(new WordCount("wang", 5), 
                                new WordCount("hai", 1), 
                                new WordCount("sheng", 1));
        wordsCount
            .keyBy("word")
            .reduce((WordCount wc1, WordCount wc2) -> new WordCount(wc1.getWord(), wc1.getCount() + wc2.getCount()))
            .print()
            .setParallelism(1);
        try {
            env.execute("Flink data type Pojo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



