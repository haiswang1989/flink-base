package com.haiswang.flink.demo.xiaoxiang.watermark.allowedLateness;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;
import com.haiswang.flink.demo.xiaoxiang.watermark.BoundedWaterMark;

/**
 * window的触发之water mark
 * 通过water mark 触发window, 允许数据延迟
 * 
 * 延缓window的清理时间
 * 
 * allowedLateness会再次触发窗口的计算,而之前触发的数据,会buffer起来,
 * 直到watermark超过end-of-window + allowedLateness()的时候,窗口才会被清理
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月3日 上午10:41:40
 */
public class TriggerWindowWithAllowedLateness extends StreamPro {
    
    /**
     * //water mark的max length为60000ms
     * //window窗口的大小为10000ms
     * //allowedLateness为10000ms
     * 
     * 窗口1的数据
     * a,1517310630000,1  
     * a,1517310631000,1
     * 
     * //触发窗口1,此时的water mark为
     * //maxEventTime-maxOutOfOrder
     * //=1517310699999-60000 
     * //=1517310639999
     * a,1517310699999,1 
     * 
     * a,1517310630000,1 //窗口1的数据,继续触发窗口1
     * /
     * 输出数据
     * this trigger has element : 1517310630000
     * this trigger has element : 1517310631000
     * this trigger has element : 1517310630000
     * /
     * a,1517310631000,1 //窗口1的数据,继续触发窗口1
     * /
     * 输出数据
     * this trigger has element : 1517310630000
     * this trigger has element : 1517310631000
     * this trigger has element : 1517310630000
     * this trigger has element : 1517310631000
     * /
     * 
     * 窗口2的数据
     * a,1517310640000,1 
     * //触发窗口2,此时的water mark为
     * //1517310709999-60000
     * //=1517310649999
     * a,1517310709999,1 
     * 
     * //窗口1的end of window timestamp : 1517310639999
     * //1517310639999 + 10000(allowedLateness) = 1517310649999 <= 1517310649999
     * //窗口1被清理了
     * //如果allowedLateness被设置为11000
     * //那么1517310639999 + 11000(allowedLateness) = 1517310650999 > 1517310649999
     * //这个时候窗口1没有被清理,那么下面的数据还是会触发窗口1
     * a,1517310630000,1 //窗口1的数据,不会触发窗口1的数据
     * 
     * 注意点:
     * 1:延迟数据可以再次触发window,数据被重复处理
     * 
     */
    @Override
    protected void run() {
        String ip = "192.168.56.101";
        int port = 8888;
        
        //设置Time类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        DataStream<String> stream = env.socketTextStream(ip, port);
        stream.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple3<String, Long, Integer> map(String line) throws Exception {
                String[] elements = line.split(",");
                return new Tuple3<String, Long, Integer>(elements[0], Long.parseLong(elements[1]), Integer.parseInt(elements[2]));
            }
        })
        .setParallelism(1)
        .assignTimestampsAndWatermarks(new BoundedWaterMark(60000l))
        .keyBy(0)
        .timeWindow(Time.seconds(10l)) //翻滚时间窗口,10S
        //window延迟10s清理,那么这个时候上一个window将会被清理,延迟数据还是会丢失
        //window延迟11s清理,那么这个时候上一个window将不会会被清理,数据会被重复处理
        //即设置超过window的大小,可以保留多个window数据?
        .allowedLateness(Time.seconds(11)) 
        .apply(new WindowFunction<Tuple3<String,Long,Integer>, Object, Tuple, TimeWindow>() {
            private static final long serialVersionUID = 1L;
            public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String,Long,Integer>> input, Collector<Object> out) throws Exception {
                System.out.println("this window is trigger, timewindow {start=" + window.getStart() + ", end=" + window.getEnd() + "}");
                for (Tuple3<String, Long, Integer> tuple3 : input) {
                    System.out.println("this trigger has element : " + tuple3.f1);
                }
            }
        }).setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        TriggerWindowWithAllowedLateness triggerWindow = new TriggerWindowWithAllowedLateness();
        triggerWindow.init();
        triggerWindow.run();
        triggerWindow.start("trigger window with allowed lateness.");
    }
}
