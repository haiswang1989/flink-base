package com.haiswang.flink.demo.xiaoxiang.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.haiswang.flink.demo.xiaoxiang.source.Record;

/**
 * 定时的watermarks
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月2日 下午4:20:21
 */
public class MyPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Record> {
    
    private static final long serialVersionUID = 1L;
    
    private final long maxOutOfOrderness = 3500l;
    
    private long currentMaxTimestamp;
    
    @Override
    public long extractTimestamp(Record element, long previousElementTimestamp) {
        long eventTimestamp = element.getTimestamp();
        currentMaxTimestamp = Math.max(eventTimestamp, previousElementTimestamp);
        return eventTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}
