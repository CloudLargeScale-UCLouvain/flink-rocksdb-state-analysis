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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.createTimestampExtractor;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

/**
 * The test program for a job that simply accumulates data in various states. This is used to stress
 * the RocksDB memory and check that the cache/write buffer management work properly, limiting the
 * overall memory footprint of RocksDB.
 */
public class RocksDBStateJoinTestProgram {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateJoinTestProgram.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);
        DataStream<Event> keyedStream1 =
                env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                        .assignTimestampsAndWatermarks(createTimestampExtractor(pt));

        DataStream<Event> keyedStream2 =
                env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                        .assignTimestampsAndWatermarks(createTimestampExtractor(pt));

        keyedStream1
                .join(keyedStream2)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(
                        new JoinFunction<Event, Event, String>() {
                            @Override
                            public String join(Event first, Event second) {
                                return first.getPayload() + "," + second.getPayload();
                            }
                        });

        env.execute("RocksDB test job");
    }

    private static class NameKeySelector implements KeySelector<Event, Integer> {
        @Override
        public Integer getKey(Event value) {
            return value.getKey();
        }
    }
}
