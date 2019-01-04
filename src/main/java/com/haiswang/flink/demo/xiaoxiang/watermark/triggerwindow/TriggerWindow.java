package com.haiswang.flink.demo.xiaoxiang.watermark.triggerwindow;

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
 * 通过water mark 触发window
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月3日 上午10:41:40
 */
public class TriggerWindow extends StreamPro {
    
    /**
     * 第一条数据: a,1517310630000,1  
     * 第二条数据: a,1517310631000,1 + 1s
     * 第三条数据: a,1517310699998,1 + 69S 998ms
     * 第四条数据: a,1517310699999,1 + 69s 999ms //触发窗口
     * 
     * 翻滚窗口的时间是10S , water mark的触发窗口的时间差是60S
     * 窗口的触发条件是水位线大于window的max time
     * 
     * 如上的数据窗口触发的条件 
     * 1517310630000(第一条数据的eventtime) - (1517310630000 + 10s + 60s - 1)
     * 
     * 1517310630000 + 10s(窗口时间) + 60s(watermark的max time) - 1(左闭又开所以减一)
     * 
     * 输出数据
     * this window is trigger, timewindow {start=1517310630000, end=1517310640000} 这边是窗口的大小是10S
     * this trigger has element : 1517310630000 第一条数据
     * this trigger has element : 1517310631000 第二条数据
     * 第三条数据不在这个窗口输出,因为第三条数据不在这个窗口范围内
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
        TriggerWindow triggerWindow = new TriggerWindow();
        triggerWindow.init();
        triggerWindow.run();
        triggerWindow.start("trigger window.");
    }
}
