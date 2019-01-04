package com.haiswang.flink.demo.xiaoxiang.window;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;
import com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup.Input1;
import com.haiswang.flink.demo.xiaoxiang.transformation.joinandcogroup.Input1Source;

/**
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月28日 下午4:34:35
 */
public class CreateWindow extends StreamPro {
    
    @Override
    protected void run() {
        
        DataStream<Input1> datastream = env.addSource(new Input1Source());
        KeyedStream<Input1, Tuple> keyedStream = datastream.keyBy("");
        
        //翻滚时间窗口
        //TumblingTimeWindow
        keyedStream.timeWindow(Time.seconds(30l));
        //滑动时间窗口
        //SlidingTimeWindow
        keyedStream.timeWindow(Time.seconds(30l), Time.seconds(10l));
        //翻滚固定长度窗口
        //CountWindow
        keyedStream.countWindow(1000l);
        //滑动固定长度窗口
        //CountWindow
        keyedStream.countWindow(1000l, 10l);
        //处理时间
        //SessionWindow
        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(20l)));
        //事件事件
        //SessionWindow
        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(20l)));
    }
    
    public static void main(String[] args) {
        
        
    }

}
