/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerOrderInfoManagerExactlyOnceTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private BrokerConfig brokerConfig;

    private ConsumerOrderInfoManager consumerOrderInfoManager;

    @Before
    public void setUp() {
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        consumerOrderInfoManager = new ConsumerOrderInfoManager(brokerController);
    }

    @Test
    public void testIsOffsetAcknowledged_WhenExactlyOnceEnabled() {
        // Given
        when(brokerConfig.isEnablePopExactlyOnce()).thenReturn(true);
        String topic = "test-topic";
        String group = "test-group";
        int queueId = 0;
        long popTime = System.currentTimeMillis();
        List<Long> offsetList = Arrays.asList(100L, 101L, 102L);
        
        // When - Update order info
        consumerOrderInfoManager.update("attempt-1", false, topic, group, queueId, popTime, 30000, offsetList, new StringBuilder());
        
        // Then - Initially no offset is acknowledged
        assertFalse(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 100L, popTime));
        assertFalse(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 101L, popTime));
        assertFalse(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 102L, popTime));
        
        // When - Acknowledge offset 101
        long nextOffset = consumerOrderInfoManager.commitAndNext(topic, group, queueId, 101L, popTime);
        
        // Then - Only offset 101 should be acknowledged
        assertFalse(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 100L, popTime));
        assertTrue(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 101L, popTime));
        assertFalse(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 102L, popTime));
        
        // Verify next offset
        assertEquals(102L, nextOffset);
    }

    @Test
    public void testGetUnacknowledgedOffsets_WhenExactlyOnceEnabled() {
        // Given
        when(brokerConfig.isEnablePopExactlyOnce()).thenReturn(true);
        String topic = "test-topic";
        String group = "test-group";
        int queueId = 0;
        long popTime = System.currentTimeMillis();
        List<Long> offsetList = Arrays.asList(100L, 101L, 102L);
        
        // When - Update order info
        consumerOrderInfoManager.update("attempt-1", false, topic, group, queueId, popTime, 30000, offsetList, new StringBuilder());
        
        // Then - All offsets should be unacknowledged initially
        List<Long> unacknowledgedOffsets = consumerOrderInfoManager.getUnacknowledgedOffsets(topic, group, queueId, popTime);
        assertEquals(3, unacknowledgedOffsets.size());
        assertTrue(unacknowledgedOffsets.contains(100L));
        assertTrue(unacknowledgedOffsets.contains(101L));
        assertTrue(unacknowledgedOffsets.contains(102L));
        
        // When - Acknowledge offset 101
        consumerOrderInfoManager.commitAndNext(topic, group, queueId, 101L, popTime);
        
        // Then - Only 100 and 102 should be unacknowledged
        unacknowledgedOffsets = consumerOrderInfoManager.getUnacknowledgedOffsets(topic, group, queueId, popTime);
        assertEquals(2, unacknowledgedOffsets.size());
        assertTrue(unacknowledgedOffsets.contains(100L));
        assertFalse(unacknowledgedOffsets.contains(101L));
        assertTrue(unacknowledgedOffsets.contains(102L));
    }

    @Test
    public void testDuplicateAckHandling_WhenExactlyOnceEnabled() {
        // Given
        when(brokerConfig.isEnablePopExactlyOnce()).thenReturn(true);
        String topic = "test-topic";
        String group = "test-group";
        int queueId = 0;
        long popTime = System.currentTimeMillis();
        List<Long> offsetList = Arrays.asList(100L, 101L, 102L);
        
        // When - Update order info and acknowledge offset 101
        consumerOrderInfoManager.update("attempt-1", false, topic, group, queueId, popTime, 30000, offsetList, new StringBuilder());
        long firstAckResult = consumerOrderInfoManager.commitAndNext(topic, group, queueId, 101L, popTime);
        
        // Then - First ack should succeed
        assertEquals(102L, firstAckResult);
        assertTrue(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 101L, popTime));
        
        // When - Try to ack the same offset again
        long secondAckResult = consumerOrderInfoManager.commitAndNext(topic, group, queueId, 101L, popTime);
        
        // Then - Second ack should return the same next offset (no change)
        assertEquals(102L, secondAckResult);
        assertTrue(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 101L, popTime));
    }

    @Test
    public void testExactlyOnceDisabled_Behavior() {
        // Given
        when(brokerConfig.isEnablePopExactlyOnce()).thenReturn(false);
        String topic = "test-topic";
        String group = "test-group";
        int queueId = 0;
        long popTime = System.currentTimeMillis();
        List<Long> offsetList = Arrays.asList(100L, 101L, 102L);
        
        // When - Update order info and acknowledge offset 101
        consumerOrderInfoManager.update("attempt-1", false, topic, group, queueId, popTime, 30000, offsetList, new StringBuilder());
        long firstAckResult = consumerOrderInfoManager.commitAndNext(topic, group, queueId, 101L, popTime);
        
        // Then - Behavior should be the same as before (no exactly once protection)
        assertEquals(102L, firstAckResult);
        assertTrue(consumerOrderInfoManager.isOffsetAcknowledged(topic, group, queueId, 101L, popTime));
        
        // When - Try to ack the same offset again
        long secondAckResult = consumerOrderInfoManager.commitAndNext(topic, group, queueId, 101L, popTime);
        
        // Then - Should still return the same next offset
        assertEquals(102L, secondAckResult);
    }
} 