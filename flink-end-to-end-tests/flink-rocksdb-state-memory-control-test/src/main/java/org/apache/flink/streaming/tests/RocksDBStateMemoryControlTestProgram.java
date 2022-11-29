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
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.applyTumblingWindows;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createTimestampExtractor;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;
import static org.apache.flink.streaming.tests.TestOperatorEnum.TIME_WINDOW_OPER;

/**
 * The test program for a job that simply accumulates data in various states. This is used to stress
 * the RocksDB memory and check that the cache/write buffer management work properly, limiting the
 * overall memory footprint of RocksDB.
 */
public class RocksDBStateMemoryControlTestProgram {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBStateMemoryControlTestProgram.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);
        final boolean useValueState = pt.getBoolean("useValueState", false);
        final boolean useListState = pt.getBoolean("useListState", false);
        final boolean useMapState = pt.getBoolean("useMapState", false);
        final double fillHeap = pt.getDouble("fillHeap", 0.0);
        final boolean replaceValue = pt.getBoolean("replaceValue", false);
        final boolean measureReadLatency = pt.getBoolean("measureReadLatency", false);
        final boolean measureWriteLatency = pt.getBoolean("measureWriteLatency", false);
        final boolean ssg = pt.getBoolean("ssg", false);
        final double ssgCPUSource = pt.getDouble("ssgCPUSource", 1.0);
        final double ssgCPUMap = pt.getDouble("ssgCPUMap", 1.0);
        final int ssgHeapSource = pt.getInt("ssgHeapSource", 500);
        final int ssgHeapMap = pt.getInt("ssgHeapMap", 100);
        final int ssgManagedSource = pt.getInt("ssgManagedSource", 0);
        final int ssgManagedMap = pt.getInt("ssgManagedMap", 500);
        final boolean readOnly = pt.getBoolean("readOnly", false);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);
        KeyedStream<Event, Integer> keyedStream;
        if (ssg) {
            SlotSharingGroup ssgSource =
                    SlotSharingGroup.newBuilder("source")
                            .setCpuCores(ssgCPUSource)
                            .setTaskHeapMemoryMB(ssgHeapSource)
                            .setManagedMemory(MemorySize.ofMebiBytes(ssgManagedSource))
                            .build();

            keyedStream =
                    env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                            .slotSharingGroup(ssgSource)
                            .name(EVENT_SOURCE.getName())
                            .uid(EVENT_SOURCE.getUid())
                            .assignTimestampsAndWatermarks(createTimestampExtractor(pt))
                            .keyBy(Event::getKey);
        } else {
            keyedStream =
                    env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                            .name(EVENT_SOURCE.getName())
                            .uid(EVENT_SOURCE.getUid())
                            .assignTimestampsAndWatermarks(createTimestampExtractor(pt))
                            .keyBy(Event::getKey);
        }

        if (useValueState) {
            if (ssg) {
                SlotSharingGroup ssgMap =
                        SlotSharingGroup.newBuilder("map")
                                .setCpuCores(ssgCPUMap)
                                .setTaskHeapMemoryMB(ssgHeapMap)
                                .setManagedMemory(MemorySize.ofMebiBytes(ssgManagedMap))
                                .build();

                keyedStream
                        .map(new ValueStateMapper(fillHeap, replaceValue, readOnly))
                        .slotSharingGroup(ssgMap)
                        .name("ValueStateMapper")
                        .uid("ValueStateMapper");
            } else {
                keyedStream
                        .map(new ValueStateMapper(fillHeap, replaceValue, readOnly))
                        .name("ValueStateMapper")
                        .uid("ValueStateMapper");
            }
        }
        if (useListState) {
            keyedStream.map(new ListStateMapper()).name("ListStateMapper").uid("ListStateMapper");
        }
        if (useMapState) {
            keyedStream.map(new MapStateMapper()).name("MapStateMapper").uid("MapStateMapper");
        }

        boolean useWindow = pt.getBoolean("useWindow", false);
        if (useWindow) {
            applyTumblingWindows(keyedStream, pt)
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

        env.execute("RocksDB test job");
    }

    private static class ValueStateMapper extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<String> valueState;

        private Vector v;

        private final double fillHeap;

        private final boolean replaceValue;

        private final boolean readOnly;

        public ValueStateMapper(double fillHeap, boolean replaceValue, boolean readOnly) {
            this.fillHeap = fillHeap;
            this.replaceValue = replaceValue;
            this.readOnly = readOnly;
        }

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "valueState-" + index, StringSerializer.INSTANCE));
            v = new Vector();
            for (int i = 0; i < 1024 * this.fillHeap; i++) { // 1GB
                v.add(new byte[1024 * 1024]); // 1MB
            }
        }

        @Override
        public Event map(Event event) throws Exception {
            String value = valueState.value();
            if (value != null && readOnly) {
                return event;
            }
            if (value != null && !replaceValue) {
                valueState.update(event.getPayload().concat(value));
            } else {
                valueState.update(event.getPayload());
            }
            return event;
        }
    }

    private static class ListStateMapper extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        private transient ListState<String> listState;

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            listState =
                    getRuntimeContext()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "listState-" + index, StringSerializer.INSTANCE));
        }

        @Override
        public Event map(Event event) throws Exception {
            listState.add(event.getPayload());
            return event;
        }
    }

    private static class MapStateMapper extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        private transient MapState<Long, String> mapState;

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            mapState =
                    getRuntimeContext()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            "mapState-" + index,
                                            LongSerializer.INSTANCE,
                                            StringSerializer.INSTANCE));
        }

        @Override
        public Event map(Event event) throws Exception {
            mapState.put(event.getSequenceNumber(), event.getPayload());
            return event;
        }
    }
}
