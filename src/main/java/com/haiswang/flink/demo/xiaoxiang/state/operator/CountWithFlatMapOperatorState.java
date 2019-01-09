package com.haiswang.flink.demo.xiaoxiang.state.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

/**
 * 
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月4日 下午5:49:45
 */
public class CountWithFlatMapOperatorState extends RichFlatMapFunction<Integer, String> implements CheckpointedFunction  {

    private static final long serialVersionUID = 1L;
    
    //flink缓存的数据,对于Operator state只保存在JVM的堆中
    //那么这个缓存是不是意味着没有效果?应用crash以后也是没法恢复的
    private transient ListState<Integer> checkPointCountList;
    
    //缓存数据的List
    private List<Integer> listBufferElements;
    
    /**
     * crash之后的恢复
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Integer> listStateDescriptor =
                new ListStateDescriptor<Integer>("listForThree", TypeInformation.of(new TypeHint<Integer>() {}));
        //注意这边在初始化的时候进行判断
        if(null == listBufferElements) {
            listBufferElements = new ArrayList<>();
        }
        
        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        for (Integer element : checkPointCountList.get()) {
            listBufferElements.add(element);
        }
    }
    
    /**
     * 进行快照
     */
    @Override
    public void snapshotState(FunctionSnapshotContext arg0) throws Exception {
        checkPointCountList.clear();
        for (Integer element : listBufferElements) {
            checkPointCountList.add(element);
        }
    }

    @Override
    public void flatMap(Integer value, Collector<String> out) throws Exception {
        if(value == 1) {
            if(listBufferElements.size() > 0) {
                StringBuilder builder = new StringBuilder();
                for (Integer integer : listBufferElements) {
                    builder.append(integer).append(" ");
                }
                out.collect(builder.toString());
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(value);
        }
    }
}
