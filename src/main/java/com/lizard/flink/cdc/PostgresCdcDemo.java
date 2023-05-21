package com.lizard.flink.cdc;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author X
 * @version 1.0
 * @since 2023-05-20 21:47
 **/
public class PostgresCdcDemo {
    public static void main(String[] args) throws Exception {
        SourceFunction<LtsJobChangeInfo> sourceFunction = PostgreSQLSource.<LtsJobChangeInfo>builder()
            .hostname("localhost")
            .port(5432)
            .database("postgres")
            .schemaList("public")
            .tableList("public.company")
            .username("postgres")
            .password("root")
            /*
             * 设置逻辑解码输出插件
             */
            .decodingPluginName("pgoutput")
            /*
             * converts SourceRecord to JSON String
             */
            .deserializer(new LtsJobDebeziumDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LtsJobChangeInfo> streamSource = env.addSource(sourceFunction)
            /*
             * use parallelism 1 for sink to keep message ordering
             */.setParallelism(1);
        streamSource.addSink(new LtsJobChangeSink());

        env.execute();
    }
}
