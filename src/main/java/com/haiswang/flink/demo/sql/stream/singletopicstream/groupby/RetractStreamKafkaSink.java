package com.haiswang.flink.demo.sql.stream.singletopicstream.groupby;

import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * 将retract stream sink到kafka
 * 
 * 
 * sink出来的数据
 * 5,wanghaiyun,1,true
 * 2,wanghaiming,1,true
 * 2,wanghaiming,1,false //又来了一个wanghaiming的数据,所以先进行del
 * 2,wanghaiming,2,true //配合上面的数据,del以后insert新的数据
 * 1,wanghaisheng,1,true
 * 2,wanghaiming,2,false //同上
 * 2,wanghaiming,3,true
 * 1,wanghaisheng,1,false //同上
 * 1,wanghaisheng,2,true
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2019年1月16日 下午2:22:25
 */
public class RetractStreamKafkaSink implements RetractStreamTableSink<Row> {
    
    /** The schema of the table. */
    private final Optional<TableSchema> schema;

    /** The Kafka topic to write to. */
    protected final String topic;

    /** Properties for the Kafka producer. */
    protected final Properties properties;

    /** Serialization schema for encoding records to Kafka. */
    protected Optional<SerializationSchema<Row>> serializationSchema;

    /** Partitioner to select Kafka partition for each item. */
    protected final Optional<FlinkKafkaPartitioner<Row>> partitioner;
    
    public RetractStreamKafkaSink(
            TableSchema schema,
            String topic,
            Properties properties,
            Optional<FlinkKafkaPartitioner<Row>> partitioner,
            SerializationSchema<Row> serializationSchema) {
        this.schema = Optional.of(Preconditions.checkNotNull(schema, "Schema must not be null."));
        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.partitioner = Preconditions.checkNotNull(partitioner, "Partitioner must not be null.");
        this.serializationSchema = Optional.of(Preconditions.checkNotNull(
            serializationSchema, "Serialization schema must not be null."));
    }
    
    /**
     * 
     * @param topic
     * @param properties
     * @param serializationSchema
     * @param partitioner
     * @return
     */
    protected SinkFunction<Row> createKafkaProducer(
        String topic,
        Properties properties,
        SerializationSchema<Row> serializationSchema,
        Optional<FlinkKafkaPartitioner<Row>> partitioner) {
        return new FlinkKafkaProducer<>(
            topic,
            new KeyedSerializationSchemaWrapper<>(serializationSchema),
            properties,
            partitioner);
    }
    
    /**
     * Return a copy of this [[TableSink]] configured with the field names and types of the
     * 通过FieldNames和fieldTypes Copy一份这个tablesink对象
     */
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }

    /**
     * table的column
     */
    @Override
    public String[] getFieldNames() {
        return schema.map(TableSchema::getFieldNames).orElse(null);
    }
    
    /**
     * table的column的type
     */
    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.map(TableSchema::getFieldTypes).orElse(null);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        SinkFunction<Row> kafkaProducer = createKafkaProducer(
                topic,
                properties,
                serializationSchema.orElseThrow(() -> new IllegalStateException("No serialization schema defined.")),
                partitioner);
        
        dataStream.map(new MapFunction<Tuple2<Boolean,Row>, Row>() {
            private static final long serialVersionUID = 1L;
            public Row map(Tuple2<Boolean,Row> value) throws Exception {
                boolean f0 = value.f0;
                Row f1 = value.f1;
                //把这边的流的类型信息添加到row中,在定义的时候
                f1.setField(f1.getArity() - 1, f0);
                return f1;
            };
        }).addSink(kafkaProducer).name(TableConnectorUtil.generateRuntimeName(this.getClass(), schema.map(TableSchema::getFieldNames).orElse(null)));
    }
    
    /**
     * Returns the type expected by this [[TableSink]].
     * This type should depend on the types returned by [[getFieldNames]].
     * 
     * DataStream的数据的类型?
     * 返回该sink期望的数据的类型,该类型需要结合getFieldNames()方法生成
     */
    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
    }
    
    /**
     * returns the requested record type
     * 返回需要的record类型
     */
    @Override
    public TypeInformation<Row> getRecordType() {
        return TypeInformation.of(Row.class);
    }
}
