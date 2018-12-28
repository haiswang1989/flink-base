package com.haiswang.flink.demo.xiaoxiang.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年12月26日 下午5:53:09
 */
public class Main {

    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //设置时间模型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        //重启策略
        //场景:checkpoint太大,不太care数据的完整性,这样可以设置不进行checkpoint
        //这样在应用cresh以后, 重试多少次, 延迟几秒后重启, 这样应用就回自动的执行restart过程
        //non checkpoint
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                Integer.MAX_VALUE, //重启尝试次数 
                Time.of(5, TimeUnit.SECONDS) //重启尝试之间的延迟
                ));
        
        //打开checkpoint,并且设置checkpoint时间间隔
        env.enableCheckpointing(5000l); 
        //两次checkpoint之间的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000l);
        //checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(5000l);
        
        //配置流
        DataStream<String> configDataStream = env.addSource(new SocketSource()).broadcast();
        //业务流
        DataStream<Record> dataStream = env.addSource(new ConnectSource());
        
        //连接流,往一个业务流中传递参数,一般可用于运行中进行配置管理
        //在不关闭应用的情况下,调整配置,业务流根据配置进行调整业务执行流程
        ConnectedStreams<Record, String> connectedStreams = dataStream.connect(configDataStream);
        
        dataStream = connectedStreams.flatMap(new CoFlatMapFunction<Record, String, Record>() {
            private static final long serialVersionUID = 1L;
            private String config;

            @Override
            public void flatMap1(Record record, Collector<Record> out) throws Exception {
                /*
                 * 业务处理
                 */
                if("0".equals(config)) {
                    out.collect(record);
                } else if("1".equals(config)) {
                    out.collect(record);
                }
            }
            
            @Override
            public void flatMap2(String s, Collector<Record> out) throws Exception {
                /*
                 * 配置处理
                 */
                config = s;
            }
        });
        
        //分流
        SplitStream<Record> splitStream = dataStream.split(new OutputSelector<Record>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<String> select(Record record) {
                List<String> output = new ArrayList<>();
                String biz = record.getBizId() + "";
                output.add(biz);
                return output;
            }
        });
        
        //1号业务线的数据
        splitStream.select("1").addSink(new SinkFunction<Record>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void invoke(Record record) throws Exception {
                //写Mysql
            }
        });
        
        //2号业务线的数据
        splitStream.select("2").addSink(new SinkFunction<Record>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void invoke(Record record) throws Exception {
                //发送Kafka
            }
        });
    }
}
