package com.haiswang.flink.demo.sql.stream.common;

import java.util.ArrayList;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月15日 下午2:47:15
 */
public class SendSource implements SourceFunction<Tuple3<Long, String, Integer>> {

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
    public void run(SourceContext<Tuple3<Long, String, Integer>> ctx) throws Exception {
        while(true) {
            int randomIndex = RANDOM.nextInt(5);
            ctx.collect(ELEMENTS.get(randomIndex));
            Thread.sleep(3000L);
        }
    }

    @Override
    public void cancel() {
        
    }
}
