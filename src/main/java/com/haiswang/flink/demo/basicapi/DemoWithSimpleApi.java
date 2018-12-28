package com.haiswang.flink.demo.basicapi;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.common.WordCount;

/**
 * 
 * 从本地文件读取数据
 * 输出带本地文件
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月13日 下午4:33:00
 */
public class DemoWithSimpleApi {

    public static void main(String[] args) throws Exception {
        
        //StreamExecutionEnvironment.createLocalEnvironment()
        //StreamExecutionEnvironment.createRemoteEnvironment(String host, int port, String... jarFiles)
        //getExecutionEnvironment()可以根据执行环境自适应的创建正确的Execution Environment
        //如果是在IDE中执行程序,那么getExecutionEnvironment会创建LocalEnvironment
        //如果是集群则会创建在cluster上执行的Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<String> text = env.readTextFile("input.txt");
        
        DataStream<WordCount> wordCounts = 
                text.flatMap(new FlatMapFunction<String, WordCount>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void flatMap(String value, Collector<WordCount> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(new WordCount(word, 1));
                        }
                    }
                }).keyBy("word")
        //.timeWindow(Time.seconds(10), Time.seconds(2))
        .reduce(new ReduceFunction<WordCount>() {
            private static final long serialVersionUID = 1L;
            @Override
            public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                // TODO Auto-generated method stub
                return new WordCount(value1.getWord(), value1.getCount() + value2.getCount());
            }
        });
        
        //wordCounts.print().setParallelism(1); //控制台打印
        wordCounts.writeAsText("output.txt").setParallelism(1); //输出到文件
        JobExecutionResult result = env.execute("demo");
        long runningTime = result.getNetRuntime(TimeUnit.SECONDS); //JOB的运行时间
        System.out.println("Job run time : " + runningTime + "(s)");
        Map<String, Object> map = result.getAllAccumulatorResults(); //累加器结果,啥玩意不知道
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
}


