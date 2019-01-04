package com.haiswang.flink.demo.xiaoxiang.time;

import org.apache.flink.streaming.api.TimeCharacteristic;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;

/**
 * 设置Flink Time类型
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月2日 上午11:36:04
 */
public class SetTime extends StreamPro {

    @Override
    protected void run() {
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //默认,事件进入算子的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //事件的生成事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime); //事件进入Flink系统的时间
    }

    public static void main(String[] args) {
        
    }

}
