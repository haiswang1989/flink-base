package com.haiswang.flink.demo.sql.stream.common;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.sources.DefinedProctimeAttribute;


/**
 * 和window一起使用
 * 添加eventtime
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 下午2:47:15
 */
public class SendSourceWithEventTime implements SourceFunction<Tuple4<Long, String, Integer, Timestamp>>, DefinedProctimeAttribute {

    private static final long serialVersionUID = 1L;
    
    private static final ArrayList<Tuple3<Long, String, Integer>> ELEMENTS = new ArrayList<>();
    
    private static final Random RANDOM = new Random();
    
    static {
        ELEMENTS.add(new Tuple3<Long, String, Integer>(1L, "wanghaisheng", 30));
        ELEMENTS.add(new Tuple3<Long, String, Integer>(2L, "wanghaiming", 32));
        ELEMENTS.add(new Tuple3<Long, String, Integer>(3L, "wanghaifei", 34));
        ELEMENTS.add(new Tuple3<Long, String, Integer>(4L, "wanghaida", 36));
        ELEMENTS.add(new Tuple3<Long, String, Integer>(5L, "wanghaiyun", 38));
    }
    
    @Override
    public void run(SourceContext<Tuple4<Long, String, Integer, Timestamp>> ctx) throws Exception {
        while(true) {
            int randomIndex = RANDOM.nextInt(5);
            Tuple3<Long, String, Integer> tuple3 = ELEMENTS.get(randomIndex);
            ctx.collect(new Tuple4<Long, String, Integer, Timestamp>(tuple3.f0, tuple3.f1, tuple3.f2, getTime()));
            Thread.sleep(3000L);
        }
    }

    @Override
    public void cancel() {
        
    }

    @Override
    public String getProctimeAttribute() {
        return "eventtime";
    }
    
    public Timestamp getTime() {
        return new Timestamp(System.currentTimeMillis());
    }
}
