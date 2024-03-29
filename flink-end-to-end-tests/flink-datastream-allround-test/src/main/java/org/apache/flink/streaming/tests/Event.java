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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/** The event type of records used in the {@link DataStreamAllroundTestProgram}. */
public class Event implements Serializable {

    private final int key;
    private final long eventTime;
    private final long sequenceNumber;
    private final String payload;

    @JsonCreator
    public Event(@JsonProperty("key") int key, @JsonProperty("eventTime") long eventTime, @JsonProperty("sequenceNumber") long sequenceNumber, @JsonProperty("payload") String payload) {
        this.key = key;
        this.eventTime = eventTime;
        this.sequenceNumber = sequenceNumber;
        this.payload = payload;
    }

    public int getKey() {
        return key;
    }

    public long getEventTime() {
        return eventTime;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event event = (Event) o;
        return key == event.key
                && eventTime == event.eventTime
                && sequenceNumber == event.sequenceNumber
                && Objects.equals(payload, event.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, eventTime, sequenceNumber, payload);
    }

    @Override
    public String toString() {
        return "Event{"
                + "key="
                + key
                + ", eventTime="
                + eventTime
                + ", sequenceNumber="
                + sequenceNumber
                + ", payload='"
                + payload
                + '\''
                + '}';
    }
}
