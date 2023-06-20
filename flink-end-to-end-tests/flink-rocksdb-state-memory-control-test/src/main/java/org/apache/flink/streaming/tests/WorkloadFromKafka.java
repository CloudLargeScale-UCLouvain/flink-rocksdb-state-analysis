package org.apache.flink.streaming.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaRatelimitSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createTimestampExtractor;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.*;
import static org.apache.flink.streaming.tests.TestOperatorEnum.DISCARDING_SINK;

public class WorkloadFromKafka {
    private static boolean replaceValue;

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);
        replaceValue = pt.getBoolean("replaceValue", false);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupEnvironment(env, pt);
        KeyedStream<Event, Integer> keyedStream;

        KafkaRatelimitSource<Event> source = KafkaRatelimitSource.<Event>builderRatelimit()
                .setBootstrapServers(pt.get("kafka-bootstrap-servers", "datagen-kafka-bootstrap.kafka:9092"))
                .setRateLimit(pt.getLong("ratelimit", 1000L))
                .setTopics("datagen")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializationSchema())
                .build();
        keyedStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                        .setParallelism(1)
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid())
                        .assignTimestampsAndWatermarks(createTimestampExtractor(pt))
                        .keyBy(Event::getKey);

        SingleOutputStreamOperator<Event> map =
                keyedStream
                        .map(new ValueStateMapper())
                        .name(VALUE_STATE_MAPPER.getName())
                        .uid(VALUE_STATE_MAPPER.getUid())
                        .rebalance()
                        .map(new CPULoadMapper(pt))
                        .name(CPU_LOAD_MAPPER.getName())
                        .uid(CPU_LOAD_MAPPER.getUid());

        if (pt.getBoolean("useWindow", false)) {
            map =
                    map.keyBy(Event::getKey)
                            .window(TumblingEventTimeWindows.of(Time.milliseconds(20L * 100L)))
                            .apply(
                                    new WindowFunction<Event, Event, Integer, TimeWindow>() {
                                        @Override
                                        public void apply(
                                                Integer integer,
                                                TimeWindow window,
                                                Iterable<Event> input,
                                                Collector<Event> out) {
                                            for (Event e : input) {
                                                out.collect(e);
                                            }
                                        }
                                    })
                            .name(TIME_WINDOW_OPER.getName())
                            .uid(TIME_WINDOW_OPER.getUid());
        }

        map.disableChaining()
                .addSink(new DiscardingSink<>())
                .name(DISCARDING_SINK.getName())
                .uid(DISCARDING_SINK.getUid());
        env.execute("RocksDB test job");
    }

    private static class ValueStateMapper extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<String> valueState;

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "valueState" + index, StringSerializer.INSTANCE));
        }

        @Override
        public Event map(Event event) throws Exception {
            if (!replaceValue) {
                String value = valueState.value();
                if (value != null) {
                    return event;
                }
            }
            valueState.update(event.getPayload());
            return event;
        }
    }

    private static class CPULoadMapper extends RichMapFunction<Event, Event> {
        private final ParameterTool params;

        public CPULoadMapper(ParameterTool params) {
            this.params = params;
        }

        // Let's waste some CPU cycles
        @Override
        public Event map(Event s) throws Exception {
            double res = 0;
            for (int i = 0; i < params.getInt("iterations", 500); i++) {
                res += Math.sin(StrictMath.cos(res)) * 2;
            }
            return s;
        }
    }

    public static class EventDeserializationSchema extends AbstractDeserializationSchema<Event> {

        private static final long serialVersionUID = 1L;

        private transient ObjectMapper objectMapper;

        @Override
        public void open(InitializationContext context) throws Exception {
            objectMapper = new ObjectMapper();
            super.open(context);
        }

        /**
         * If our deserialize method needed access to the information in the Kafka headers of a
         * KafkaConsumerRecord, we would have implemented a KafkaRecordDeserializationSchema instead of
         * extending AbstractDeserializationSchema.
         */
        @Override
        public Event deserialize(byte[] message) throws IOException {
            return objectMapper.readValue(message, Event.class);
        }
    }
}
