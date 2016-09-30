/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.writer;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the AckCalculator class.
 */
public class AckCalculatorTests {
    /**
     * Tests the getHighestCommittedSequenceNumber method.
     */
    @Test
    public void testGetHighestCommittedSequenceNumber() {
        final long initialLastReadSeqNo = 1;
        final int processorCount = 10;
        WriterState state = new WriterState();
        AckCalculator calc = new AckCalculator(state);
        Random random = new Random();

        ArrayList<TestProcessor> processors = new ArrayList<>();
        for (int i = 0; i < processorCount; i++) {
            processors.add(new TestProcessor());
        }

        // Part 1: make sure WriterState.LastReadSequenceNumber is not exceeded.
        state.setLastReadSequenceNumber(initialLastReadSeqNo);

        // 1a. Empty set
        long result = calc.getHighestCommittedSequenceNumber(new ArrayList<>());
        Assert.assertEquals("Unexpected result for Empty Set when LRSN is small.", state.getLastReadSequenceNumber(), result);

        // 1b. None have values
        processors.forEach(p -> p.setLowestUncommittedSequenceNumber(-1));
        result = calc.getHighestCommittedSequenceNumber(new ArrayList<>());
        Assert.assertEquals("Unexpected result for Set with no values when LRSN is small.", state.getLastReadSequenceNumber(), result);

        // 1c. All have values
        processors.forEach(p -> p.setLowestUncommittedSequenceNumber(initialLastReadSeqNo + 1 + random.nextInt(1000)));
        result = calc.getHighestCommittedSequenceNumber(processors);
        Assert.assertEquals("Unexpected result for Set with values when LRSN is small.", state.getLastReadSequenceNumber(), result);

        // Part 2: WriterState.LastReadSequenceNumber is a high value, and thus is a no-factor.
        state.setLastReadSequenceNumber(Long.MAX_VALUE);

        // 2a. Empty set
        result = calc.getHighestCommittedSequenceNumber(new ArrayList<>());
        Assert.assertEquals("Unexpected result for Empty Set when LRSN is infinite.", state.getLastReadSequenceNumber(), result);

        // 2b. None have values
        processors.forEach(p -> p.setLowestUncommittedSequenceNumber(-1));
        result = calc.getHighestCommittedSequenceNumber(new ArrayList<>());
        Assert.assertEquals("Unexpected result for Set with no values when LRSN is infinite.", Long.MAX_VALUE, result);

        // 2c. All have values
        processors.forEach(p -> p.setLowestUncommittedSequenceNumber(initialLastReadSeqNo + 1 + random.nextInt(1000)));
        result = calc.getHighestCommittedSequenceNumber(processors);
        long expectedResult = processors.stream().mapToLong(TestProcessor::getLowestUncommittedSequenceNumber).min().getAsLong() - 1;
        Assert.assertEquals("Unexpected result for Set with values when LRSN is infinite.", expectedResult, result);

        // 2d. Some have values, some don't
        AtomicInteger resetCount = new AtomicInteger();
        processors.forEach(p -> {
            if (random.nextBoolean()) {
                p.setLowestUncommittedSequenceNumber(-1);
                resetCount.incrementAndGet();
            }
        });
        Assert.assertTrue("Either no resets or all were reset. Bad test.", 0 < resetCount.get() && resetCount.get() < processors.size());
        result = calc.getHighestCommittedSequenceNumber(processors);
        expectedResult = processors.stream().mapToLong(TestProcessor::getLowestUncommittedSequenceNumber).filter(sn -> sn >= 0).min().getAsLong() - 1;
        Assert.assertEquals("Unexpected result for Set with partial values when LRSN is infinite.", expectedResult, result);
    }

    private static class TestProcessor implements OperationProcessor {
        private long lowestUncommittedSequenceNumber;

        void setLowestUncommittedSequenceNumber(long value) {
            this.lowestUncommittedSequenceNumber = value;
        }

        @Override
        public long getLowestUncommittedSequenceNumber() {
            return this.lowestUncommittedSequenceNumber;
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }
}