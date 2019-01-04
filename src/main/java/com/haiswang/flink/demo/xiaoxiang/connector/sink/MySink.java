package com.haiswang.flink.demo.xiaoxiang.connector.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.haiswang.flink.demo.xiaoxiang.source.Record;

/**
 * 自定义的sink
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月4日 上午10:42:51
 */
public class MySink implements SinkFunction<Record>, CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    
    private transient ListState<Record> checkpointedState;
    
    //
    private List<Record> elements;
    
    private int threshold;
    
    public MySink(int thresholdArgs) {
        this.threshold = thresholdArgs;
        elements = new ArrayList<>();
    }
    
    /**
     * sink数据
     */
    @Override
    public void invoke(Record record) throws Exception {
        elements.add(record);
        if(elements.size() == threshold) {
            for (Record element : elements) {
                sinkData(element);
            }
            elements.clear();
        }
    }
    
    /**
     * 给数据做备份,用于crash时的容错
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Record record : elements) {
            checkpointedState.add(record);
        }
    }
    
    /**
     * 恢复数据
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Record> descriptor = new ListStateDescriptor<>("my-sink", TypeInformation.of(new TypeHint<Record>() {}));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if(context.isRestored()) {
            for (Record record : checkpointedState.get()) {
                elements.add(record);
            }
        }
    }
    
    /**
     * sink数据
     * @param record
     */
    public void sinkData(Record record) {
    }
}
