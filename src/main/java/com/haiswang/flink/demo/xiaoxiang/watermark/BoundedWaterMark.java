package com.haiswang.flink.demo.xiaoxiang.watermark;

import java.text.SimpleDateFormat;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月3日 上午10:24:27
 */                                      
public class BoundedWaterMark implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>> {
    
    private static final long serialVersionUID = 1L;
    
    private long maxOutOfOrder;
    
    private long currentMaxTimestamp;
    
    public BoundedWaterMark(long maxOutOfOrderArgs) {
        this.maxOutOfOrder = maxOutOfOrderArgs;
    }
    
    @Override
    public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long timestamp = element.f1;
        String date = sdf.format(timestamp);
        System.out.println("timestamp : " + timestamp + ", date : " + date + ", lasttimestamp : " + previousElementTimestamp);
        currentMaxTimestamp = Math.max(timestamp, previousElementTimestamp);
        return timestamp;
    }
    
    /**
     * currentMaxTimestamp - maxOutOfOrder 大于 window的大小就触发?
     */
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrder);
    }
}
