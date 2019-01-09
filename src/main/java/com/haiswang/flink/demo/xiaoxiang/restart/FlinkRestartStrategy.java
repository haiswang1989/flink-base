package com.haiswang.flink.demo.xiaoxiang.restart;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * Flink应用的重启策略
 * 
 * 针对没有设置checkpoint的任务
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月8日 下午3:42:25
 */
public class FlinkRestartStrategy extends StreamPro {

    @Override
    protected void run() {
        //尝试3次
        //每次尝试之间的时间间隔为10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        
        //无需尝试
        env.setRestartStrategy(RestartStrategies.noRestart());
        
        //
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
    }
    
    public static void main(String[] args) {
        
    }
    
}
