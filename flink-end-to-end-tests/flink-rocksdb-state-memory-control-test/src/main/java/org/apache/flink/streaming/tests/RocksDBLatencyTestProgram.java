package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createTimestampExtractor;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;

public class RocksDBLatencyTestProgram {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLatencyTestProgram.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);
        KeyedStream<Event, Integer> keyedStream =
                env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid())
                        .assignTimestampsAndWatermarks(createTimestampExtractor(pt))
                        .keyBy(Event::getKey);
        keyedStream.map(new ValueStateMapper()).name("ValueStateMapper").uid("ValueStateMapper");
        env.execute("RocksDB test job");
    }

    private static class ValueStateMapper extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<String> valueState;

        private List<Double> measurements = new ArrayList<>();

        private int counter = 0;

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "valueState-" + index, StringSerializer.INSTANCE));
        }

        @Override
        public void close() {
            measurements.forEach((l) -> LOG.info(String.valueOf(l)));
        }

        @Override
        public Event map(Event event) throws Exception {
            long startTime = System.nanoTime();
            String value = valueState.value();
            if (counter % 100 == 0) {
                LOG.info(String.valueOf(System.nanoTime() - startTime));
            }
            if (value != null) {
                valueState.update(event.getPayload().concat(value));
            } else {
                valueState.update(event.getPayload());
            }
            counter++;
            return event;
        }
    }
}
