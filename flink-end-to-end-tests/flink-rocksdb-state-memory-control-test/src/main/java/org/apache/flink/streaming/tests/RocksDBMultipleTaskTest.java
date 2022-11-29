/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.*;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;
import static org.apache.flink.streaming.tests.TestOperatorEnum.TIME_WINDOW_OPER;

/**
 * The test program for a job that simply accumulates data in various states. This is used to stress
 * the RocksDB memory and check that the cache/write buffer management work properly, limiting the
 * overall memory footprint of RocksDB.
 */
public class RocksDBMultipleTaskTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);
        KeyedStream<Event, Integer> keyedStream;
        keyedStream =
                env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                        .setParallelism(1)
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid())
                        .assignTimestampsAndWatermarks(createTimestampExtractor(pt))
                        .keyBy(Event::getKey);

        keyedStream
                .map(new ValueStateMapper())
                .name("ValueStateMapper")
                .uid("ValueStateMapper")
                .keyBy(Event::getKey)
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
                .uid(TIME_WINDOW_OPER.getUid())
                .disableChaining();

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
            String value = valueState.value();
            if (value != null) {
                return event;
            }
            valueState.update(event.getPayload());
            return event;
        }
    }
}
