package org.apache.flink.streaming.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.*;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;

public class DatagenKafka {
    private static final Logger LOG =
            LoggerFactory.getLogger(DatagenKafka.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSink<Event> sink = KafkaSink.<Event>builder()
                .setBootstrapServers(pt.get("kafka-bootstrap-servers", "datagen-kafka-bootstrap.kafka:9092"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<Event>builder()
                                .setTopic("datagen")
                                .setValueSerializationSchema(new EventSerializationSchema())
                                //.setKafkaValueSerializer(EventSerializer.class)
                                .build()
                )
                .build();

        setupEnvironment(env, pt);
        env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                .name(EVENT_SOURCE.getName())
                .uid(EVENT_SOURCE.getUid())
                .assignTimestampsAndWatermarks(createTimestampExtractor(pt))
                .sinkTo(sink)
                ;
        env.setParallelism(4);
        env.execute("RocksDB test job");
    }

    public static class EventSerializationSchema implements SerializationSchema<Event> {

        private static final long serialVersionUID = 1L;

        private transient ObjectMapper objectMapper;

        @Override
        public void open(InitializationContext context) throws Exception {
            objectMapper = new ObjectMapper();
            SerializationSchema.super.open(context);
        }

        @Override
        public byte[] serialize(Event event) {
            byte[] retVal = null;
            try {
                retVal = objectMapper.writeValueAsString(event).getBytes();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return retVal;
        }
    }
}
