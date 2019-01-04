package com.haiswang.flink.demo.xiaoxiang.watermark.sideoutputtag;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.haiswang.flink.demo.xiaoxiang.transformation.StreamPro;
import com.haiswang.flink.demo.xiaoxiang.watermark.BoundedWaterMark;

/**
 * 通过sideOutputTag对延迟数据进行处理,一般情况是开辟一个新的流,
 * 通过一个新的管道对延迟数据进行输出,进行问题的定位于排查
 * 
 * 第一条数据:a,1517310630000,1
 * 第二条数据:a,1517310631000,1
 * 第三条数据:a,1517310699999,1 //触发窗口
 * 第四条数据:a,1517310630000,1 //延迟数据
 * 第五条数据:a,1517310631000,1 //延迟数据
 * 
 * 输出结果：
 * this window is trigger, timewindow {start=1517310630000, end=1517310640000}
 * this trigger has element : 1517310630000
 * this trigger has element : 1517310631000
 * 
 * (a,1517310630000,1) //这个是通过sideOutputLateData流进行输出的
 * (a,1517310631000,1) //这个是通过sideOutputLateData流进行输出的
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月3日 下午1:41:05
 */
public class TriggerWindowWithSideOuputTag extends StreamPro {

    @Override
    protected void run() {
        String ip = "192.168.56.101";
        int port = 8888;
        
        //设置Time类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        OutputTag<Tuple3<String,Long,Integer>> outputTag = new OutputTag<Tuple3<String,Long,Integer>>("output-tag") {
            private static final long serialVersionUID = 1L;
        };
        
        DataStream<String> stream = env.socketTextStream(ip, port);
        SingleOutputStreamOperator<Object> result = stream.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
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
        .timeWindow(Time.seconds(10l))
        .sideOutputLateData(outputTag) //设置outputTag
        .apply(new WindowFunction<Tuple3<String,Long,Integer>, Object, Tuple, TimeWindow>() {
            private static final long serialVersionUID = 1L;
            public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String,Long,Integer>> input, Collector<Object> out) throws Exception {
                System.out.println("this window is trigger, timewindow {start=" + window.getStart() + ", end=" + window.getEnd() + "}");
                for (Tuple3<String, Long, Integer> tuple3 : input) {
                    System.out.println("this trigger has element : " + tuple3.f1);
                }
            }
        }).setParallelism(1);
        
        //对延迟数据进行打印
        DataStream<Tuple3<String,Long,Integer>> delayTuples = result.getSideOutput(outputTag);
        delayTuples.print().setParallelism(1);
    }
    
    public static void main(String[] args) throws Exception {
        TriggerWindowWithSideOuputTag triggerWindow = new TriggerWindowWithSideOuputTag();
        triggerWindow.init();
        triggerWindow.run();
        triggerWindow.start("trigger window with side output tag.");
    }
    
}
