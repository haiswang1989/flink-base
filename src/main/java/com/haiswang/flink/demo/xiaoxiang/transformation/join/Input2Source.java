package com.haiswang.flink.demo.xiaoxiang.transformation.join;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Input2Source implements SourceFunction<Input2> {
    
    private static final long serialVersionUID = 1L;
    
    private Random random = new Random();
    
    @Override
    public void run(SourceContext<Input2> ctx)
            throws Exception {
        while(true) {
            int randInt = random.nextInt();
            ctx.collect(new Input2((randInt % 5) + "", randInt + ""));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        
    }
}
