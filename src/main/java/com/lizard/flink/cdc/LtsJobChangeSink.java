package com.lizard.flink.cdc;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author X
 * @version 1.0
 * @since 2023-05-20 23:33
 **/
public class LtsJobChangeSink implements SinkFunction<LtsJobChangeInfo> {
    private static final long serialVersionUID = 3834521835450161357L;

    @Override
    public void invoke(LtsJobChangeInfo value, Context context) throws Exception {
        System.out.println("value = " + value);
        System.out.println("context = " + context);
    }
}
