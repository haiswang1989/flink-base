package com.haiswang.flink.demo.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月19日 上午10:54:35
 */
public class DataSetWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements("Who's there?", "I think I hear them. Stand, ho! Who's there?");
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
        .groupBy(0)
        .sum(1)
        .print();
    }

}
