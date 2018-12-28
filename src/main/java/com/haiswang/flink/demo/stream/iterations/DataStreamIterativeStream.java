package com.haiswang.flink.demo.stream.iterations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 这个程序原本的逻辑是生成0-10000之间的元素,然后在map阶段-1以后返回
 * 
 * 后来加了iteration的逻辑,就是当value的值大于0时被feedback流再次丢到stream的头部
 * 这样直到value<=0时才不被加入到feedback中,最后在filter中过滤
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月19日 上午9:50:50
 */
public class DataStreamIterativeStream {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> dStream = env.generateSequence(0, 10000);
        
        //第一步：定义一个IterativeStream
        IterativeStream<Long> itStream = dStream.iterate();
        
        //第二步：指定流中需要执行的"转换"操作
        DataStream<Long> minusOne = itStream.map(new MapFunction<Long, Long>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Long map(Long value) throws Exception {
                return value-1;
            }
        });
        
        //feedbackstream
        //该stream必须定义,否则程序会报错
        //该stream就是制定stream中哪一部分需要反馈到iteration(value > 0)的部分
        //只能使用filter和split?
        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            private static final long serialVersionUID = 1L;
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        
        //第三步：
        //调用closeWith方法
        //入参的stream将会被反馈到stream的头部
        itStream.closeWith(stillGreaterThanZero);
        
        //第四步：
        //通常情况下一般使用filter来分离feedback流和正常往前传播的流
        //同时该filter也可以定义"终止"逻辑,其中允许元素向下游传播而不是反馈。
        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            private static final long serialVersionUID = 1L;
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });
        
        lessThanZero.print().setParallelism(1);
        
        try {
            env.execute("Iterative Stream.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
