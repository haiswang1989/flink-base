package com.haiswang.flink.demo.stream.latencycontrol;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink的延迟控制
 * 
 * 在Flink程序中,默认情况下,数据不是一条一条的在网络中传输的(导致不必要的网络流量,造成网络的压力)
 * Flink是通过buffer来缓冲数据,然后通过一个buffer一个buffer的在机器中间传输的,buffer的大小程序可以控制
 * 通过buffer的方式,可以优化吞吐量,但是当数据流量不是很快的时候他也会产生数据延迟问题
 *
 * 通可以通过env.setBufferTimeout(timeoutMillis)控制吞吐量和延迟
 * buffer + 超时机制控制
 * 设置超时时间去等待buffer被"填充",当超过了timeout设置的时间,就算buffer没有满,也会发送到下一个节点
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年11月19日 上午10:34:59
 */
public class DataStreamLatencyControl {

    public static void main(String[] args) {
        long timeoutMillis = 200;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(timeoutMillis);
        
        env.generateSequence(1, 100).map(new MapFunction<Long, Long>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Long map(Long value) throws Exception {
                return value-1;
            }
        }).setBufferTimeout(timeoutMillis).print().setParallelism(1);
        
        try {
            env.execute("Buffer time out.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
