package com.lizard.flink.cdc;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author X
 * @version 1.0
 * @since 2023-05-20 22:20
 **/
public class LtsJobDebeziumDeserializationSchema implements DebeziumDeserializationSchema<LtsJobChangeInfo> {
    private static final long serialVersionUID = -6814054397977785244L;
    private static final Logger log = LoggerFactory.getLogger(LtsJobDebeziumDeserializationSchema.class);

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<LtsJobChangeInfo> collector) throws Exception {
        final String topic = sourceRecord.topic();
        log.debug("收到{}的消息，准备进行转换", topic);

        final LtsJobChangeInfo dataChangeInfo = new LtsJobChangeInfo();

        System.out.println("sourceRecord = " + sourceRecord);

        Object value = sourceRecord.value();
        System.out.println("value = " + value);
        Schema schema = sourceRecord.valueSchema();
        System.out.println("schema = " + schema);
        Long timestamp = sourceRecord.timestamp();
        System.out.println("timestamp = " + timestamp);

        //        final Struct struct = (Struct) sourceRecord.value();
//        final Struct source = struct.getStruct(SOURCE);
//        dataChangeInfo.setBeforeData( getDataJsonString(struct, BEFORE));
//        dataChangeInfo.setAfterData(getDataJsonString(struct, AFTER));

        //5.获取操作类型  CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        dataChangeInfo.setOperation(operation.toString().toLowerCase());
//        dataChangeInfo.setDatabase(Optional.ofNullable(source.get(DATABASE)).map(Object::toString).orElse(""));
//        dataChangeInfo.setSchema(Optional.ofNullable(source.get(SCHEMA)).map(Object::toString).orElse(""));
//        dataChangeInfo.setTableName(Optional.ofNullable(source.get(TABLE)).map(Object::toString).orElse(""));
//        dataChangeInfo.setChangeTime(Optional.ofNullable(struct.get(TS_MS)).map(x -> Long.parseLong(x.toString())).orElseGet(System::currentTimeMillis));


        log.info("收到{}的{}类型的消息， 已经转换好了，准备发往sink", topic, dataChangeInfo.getOperation());
        //7.输出数据
        collector.collect(dataChangeInfo);
    }

    @Override
    public TypeInformation<LtsJobChangeInfo> getProducedType() {
        return TypeInformation.of(LtsJobChangeInfo.class);
    }
}
