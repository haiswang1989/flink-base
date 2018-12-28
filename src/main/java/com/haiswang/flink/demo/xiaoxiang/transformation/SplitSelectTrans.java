package com.haiswang.flink.demo.xiaoxiang.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;

/**
 * split算子将流一拆多
 * select算子选择指定的流
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月27日 上午10:50:29
 */
public class SplitSelectTrans extends StreamPro {

    @Override
    protected void run() {
        
        DataStream<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6);
        SplitStream<Integer> split = intStream.split(new OutputSelector<Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<>();
                if(value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                
                return output;
            }
        });
        
        //偶数流
        DataStream<Integer> evenStream = split.select("even");
        evenStream.map(new MapFunction<Integer, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String map(Integer value) throws Exception {
                return "even : " + value;
            }
        }).print().setParallelism(1);
        
        //奇数流
        DataStream<Integer> oddStream = split.select("odd");
        oddStream.map(new MapFunction<Integer, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String map(Integer value) throws Exception {
                return "odd : " + value;
            }
        }).print().setParallelism(1);
        
        //全部的数据流
        DataStream<Integer> allStream = split.select("even", "odd");
        allStream.map(new MapFunction<Integer, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String map(Integer value) throws Exception {
                return "all : " + value;
            }
        }).print().setParallelism(1);
        
    }
    
    public static void main(String[] args) throws Exception {
        SplitSelectTrans splitSelectTrans = new SplitSelectTrans();
        splitSelectTrans.init();
        splitSelectTrans.run();
        splitSelectTrans.start("transformation split and select.");
    }

}
