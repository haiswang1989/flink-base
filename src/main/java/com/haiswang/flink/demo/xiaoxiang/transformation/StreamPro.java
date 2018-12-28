package com.haiswang.flink.demo.xiaoxiang.transformation;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class StreamPro {
    
    protected StreamExecutionEnvironment env;
    
    protected void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
    
    protected abstract void run();
    
    protected void start(String jobName) throws Exception {
        env.execute(jobName);
    }
    
}
