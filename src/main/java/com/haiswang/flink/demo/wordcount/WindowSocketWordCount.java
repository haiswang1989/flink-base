package com.haiswang.flink.demo.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年10月23日 下午5:41:38
 */
public class WindowSocketWordCount {

    public static void main(String[] args) throws Exception {
        
        int socketPort = 9000;
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStreamSource是DataStream的子类
        DataStream<String> text = env.socketTextStream("192.168.56.101", socketPort, "\n");
        
        //flatMap是将"一个类型的对象"转换成"另一个类型的对象"
        DataStream<WordWithCount> windowWordCount = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void flatMap(String lineContext, Collector<WordWithCount> out) throws Exception {
                        for (String word : lineContext.split(" ")) {
                            out.collect(new WordWithCount(word, 1l));
                        }
                    }
                })
                .keyBy("word")
                //统计时间窗口是5S
                //滑动周期是1S
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });
        
        windowWordCount.print().setParallelism(1);
        env.execute("Socket window word count.");
    }
    
    /**
     * 
     * <p>Description:</p>
     * @author hansen.wang
     * @date 2018年10月23日 下午5:12:55
     */
    public static class WordWithCount {
        
        public String word;
        public long count;
        
        //如果没有这个无参构造函数
        //那么在启动任务的时候会报如下错误
        //org.apache.flink.api.common.InvalidProgramException: 
        //This type (GenericType<com.haiswang.flink.demo.WindowSocketWordCount.WordWithCount>) 
        //cannot be used as key.
        
        //分析原因
        //没有这个无参构造函数,该类会被识别为GenericType, 如果有这个无参构造函数会被识别为PojoTypeInfo
        //GenericType
        //PojoTypeInfo集成自CompositeType
        //Pojo对象,即所有的成员要么是Public的,那么都有Getter和Setter方法
        public WordWithCount() {}
        
        public WordWithCount(String wordArgs, long countArgs) {
            this.word = wordArgs;
            this.count = countArgs;
        }
        
        @Override
        public String toString() {
            return word + ":" + count;
        }
    }
}
