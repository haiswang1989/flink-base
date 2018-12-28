package com.haiswang.flink.demo.accumulators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.common.WordCount;

/**
 * 统计wordcount的数据的行数
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月16日 上午9:49:13
 */
public class CountLineInWordCount {

    public static void main(String[] args) throws Exception {
        
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> lines = env.readTextFile("input.txt");
        
        lines.flatMap(new RichFlatMapFunction<String, WordCount>() {
            private static final long serialVersionUID = 1L;
            private IntCounter lineNums = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("line-num", lineNums);
            }
            
            @Override
            public void flatMap(String line, Collector<WordCount> out) throws Exception {
                lineNums.add(1);
                for (String word : line.split(" ")) {
                    out.collect(new WordCount(word, 1));
                }
            }
        })
        .groupBy("word")
        .reduce(new ReduceFunction<WordCount>() {
            private static final long serialVersionUID = 1L;
            @Override
            public WordCount reduce(WordCount wc1, WordCount wc2) throws Exception {
                return new WordCount(wc1.getWord(), wc1.getCount() + wc2.getCount());
            }
        }).setParallelism(1).print();
        
        JobExecutionResult result = env.getLastJobExecutionResult();
        int allLineNums = result.getAccumulatorResult("line-num");
        System.out.println("文件的行数 : " + allLineNums);
    }

}
