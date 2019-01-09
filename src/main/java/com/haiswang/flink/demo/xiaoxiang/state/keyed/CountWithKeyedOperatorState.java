package com.haiswang.flink.demo.xiaoxiang.state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月7日 下午4:37:03
 */
public class CountWithKeyedOperatorState extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
    
    private static final long serialVersionUID = 1L;
    
    private transient ValueState<Tuple2<Integer, Integer>> sum;
    
    @Override
    public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
        Tuple2<Integer, Integer> sumTuple = sum.value();
        if(null == sumTuple) {
            sumTuple = new Tuple2<Integer, Integer>(0, 0);
        }
        
        //元素个数
        sumTuple.f0 += 1;
        //新的sum值
        sumTuple.f1 += value.f1;
        //更新,这边在进行snapshot的时候,不需要像Operator state那样必须实现CheckpointedFunction方法
        //实时存储到rocksdb上面
        sum.update(sumTuple);
        if(sumTuple.f0 == 3) {
            out.collect(new Tuple2<Integer, Integer>(value.f0, sumTuple.f1 / sumTuple.f0));
            sum.clear();
        }
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<Tuple2<Integer, Integer>>("average", TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}));
        sum = getRuntimeContext().getState(descriptor);
    }
    
}