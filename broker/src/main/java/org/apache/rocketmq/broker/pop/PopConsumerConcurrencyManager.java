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
package org.apache.rocketmq.broker.pop;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * POP消费并发性管理器
 * 实现不同consumer group的正交性判断和并发控制
 */
public class PopConsumerConcurrencyManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    
    private final BrokerController brokerController;
    
    // 队列级别的并发控制：topic@queueId -> Set<consumerGroup>
    private final Map<String, Set<String>> queueConsumerGroups = new ConcurrentHashMap<>();
    
    // 消费者组级别的并发计数：consumerGroup -> AtomicInteger
    private final Map<String, AtomicInteger> consumerGroupConcurrency = new ConcurrentHashMap<>();
    
    public PopConsumerConcurrencyManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    
    /**
     * 检查是否可以并发消费
     */
    public boolean canConcurrentConsume(String topic, int queueId, String consumerGroup) {
        String queueKey = buildQueueKey(topic, queueId);
        Set<String> existingGroups = queueConsumerGroups.get(queueKey);
        
        if (existingGroups == null || existingGroups.isEmpty()) {
            return true;
        }
        
        // 检查是否与现有消费者组正交
        for (String existingGroup : existingGroups) {
            if (!isOrthogonal(consumerGroup, existingGroup)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 注册消费者组到队列
     */
    public boolean registerConsumerGroup(String topic, int queueId, String consumerGroup) {
        String queueKey = buildQueueKey(topic, queueId);
        
        return queueConsumerGroups.compute(queueKey, (key, groups) -> {
            if (groups == null) {
                groups = ConcurrentHashMap.newKeySet();
            }
            
            if (canConcurrentConsume(topic, queueId, consumerGroup)) {
                groups.add(consumerGroup);
                incrementConsumerGroupConcurrency(consumerGroup);
                return groups;
            } else {
                return groups;
            }
        }) != null;
    }
    
    /**
     * 注销消费者组从队列
     */
    public void unregisterConsumerGroup(String topic, int queueId, String consumerGroup) {
        String queueKey = buildQueueKey(topic, queueId);
        
        queueConsumerGroups.computeIfPresent(queueKey, (key, groups) -> {
            groups.remove(consumerGroup);
            decrementConsumerGroupConcurrency(consumerGroup);
            
            if (groups.isEmpty()) {
                return null;
            }
            return groups;
        });
    }
    
    /**
     * 检查两个消费者组是否正交
     */
    private boolean isOrthogonal(String group1, String group2) {
        if (group1.equals(group2)) {
            return false;
        }
        
        // 默认情况下，不同消费者组可以并发
        return true;
    }
    
    /**
     * 增加消费者组并发计数
     */
    private void incrementConsumerGroupConcurrency(String consumerGroup) {
        consumerGroupConcurrency.computeIfAbsent(consumerGroup, k -> new AtomicInteger(0))
            .incrementAndGet();
    }
    
    /**
     * 减少消费者组并发计数
     */
    private void decrementConsumerGroupConcurrency(String consumerGroup) {
        AtomicInteger counter = consumerGroupConcurrency.get(consumerGroup);
        if (counter != null) {
            int newValue = counter.decrementAndGet();
            if (newValue <= 0) {
                consumerGroupConcurrency.remove(consumerGroup);
            }
        }
    }
    
    /**
     * 构建队列键
     */
    private String buildQueueKey(String topic, int queueId) {
        return topic + "@" + queueId;
    }
} 