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
package org.apache.rocketmq.proxy.service.route;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.latency.Resolver;
import org.apache.rocketmq.client.latency.ServiceDetector;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.AbstractCacheLoader;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class TopicRouteService extends AbstractStartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final MQClientAPIFactory mqClientAPIFactory;
    private MQFaultStrategy mqFaultStrategy;

    protected final LoadingCache<String /* topicName */, MessageQueueView> topicCache;
    protected final ScheduledExecutorService scheduledExecutorService;
    protected final ThreadPoolExecutor cacheRefreshExecutor;
    private final TopicRouteCacheLoader topicRouteCacheLoader = new TopicRouteCacheLoader();


    public TopicRouteService(MQClientAPIFactory mqClientAPIFactory) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("TopicRouteService_")
        );
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getTopicRouteServiceThreadPoolNums(),
            config.getTopicRouteServiceThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "TopicRouteCacheRefresh",
            config.getTopicRouteServiceThreadPoolQueueCapacity()
        );
        this.mqClientAPIFactory = mqClientAPIFactory;

        this.topicCache = Caffeine.newBuilder().maximumSize(config.getTopicRouteServiceCacheMaxNum()).
            refreshAfterWrite(config.getTopicRouteServiceCacheExpiredInSeconds(), TimeUnit.SECONDS).
            executor(cacheRefreshExecutor).build(new CacheLoader<String, MessageQueueView>() {
                @Override public @Nullable MessageQueueView load(String topic) throws Exception {
                    try {
                        TopicRouteData topicRouteData = topicRouteCacheLoader.loadTopicRouteData(topic);
                        if (isTopicRouteValid(topicRouteData)) {
                            MessageQueueView tmp = new MessageQueueView(topic, topicRouteData);
                            log.info("load topic route from namesrv. topic: {}, queue: {}", topic, tmp);
                            return tmp;
                        }
                        return MessageQueueView.WRAPPED_EMPTY_QUEUE;
                    } catch (Exception e) {
                        if (TopicRouteHelper.isTopicNotExistError(e)) {
                            return MessageQueueView.WRAPPED_EMPTY_QUEUE;
                        }
                        throw e;
                    }
                }

                @Override public @Nullable MessageQueueView reload(@NonNull String key,
                    @NonNull MessageQueueView oldValue) throws Exception {
                    try {
                        return load(key);
                    } catch (Exception e) {
                        log.warn(String.format("reload topic route from namesrv. topic: %s", key), e);
                        return oldValue;
                    }
                }
            });
        ServiceDetector serviceDetector = new ServiceDetector() {
            @Override
            public boolean detect(String endpoint, long timeoutMillis) {
                Optional<String> candidateTopic = pickTopic();
                if (!candidateTopic.isPresent()) {
                    return false;
                }
                try {
                    GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
                    requestHeader.setTopic(candidateTopic.get());
                    requestHeader.setQueueId(0);
                    Long maxOffset = mqClientAPIFactory.getClient().getMaxOffset(endpoint, requestHeader,timeoutMillis).get();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        };
        mqFaultStrategy = new MQFaultStrategy(extractClientConfigFromProxyConfig(config), new Resolver() {
            @Override
            public String resolve(String name) {
                try {
                    String brokerAddr = getBrokerAddr(name);
                    return brokerAddr;
                } catch (Exception e) {
                    return null;
                }
            }
        }, serviceDetector);
        this.init();
    }

    protected void init() {
        this.appendShutdown(this.scheduledExecutorService::shutdown);
        this.appendStartAndShutdown(this.mqClientAPIFactory);
    }
    // pickup one topic in the topic cache
    private Optional<String> pickTopic() {
        if (topicCache.asMap().isEmpty()) {
            return Optional.absent();
        }
        return Optional.of(topicCache.asMap().keySet().iterator().next());
    }

    @Override
    public void shutdown() throws Exception {
        if (this.mqFaultStrategy.isStartDetectorEnable()) {
            mqFaultStrategy.shutdown();
        }
    }

    @Override
    public void start() throws Exception {
        if (this.mqFaultStrategy.isStartDetectorEnable()) {
            this.mqFaultStrategy.startDetector();
        }
    }

    public ClientConfig extractClientConfigFromProxyConfig(ProxyConfig proxyConfig) {
        ClientConfig tempClientConfig = new ClientConfig();
        tempClientConfig.setSendLatencyEnable(proxyConfig.getSendLatencyEnable());
        tempClientConfig.setStartDetectorEnable(proxyConfig.getStartDetectorEnable());
        tempClientConfig.setDetectTimeout(proxyConfig.getDetectTimeout());
        tempClientConfig.setDetectInterval(proxyConfig.getDetectInterval());
        return tempClientConfig;
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation,
                                boolean reachable) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation, reachable);
    }

    public MQFaultStrategy getMqFaultStrategy() {
        return this.mqFaultStrategy;
    }

    public MessageQueueView getAllMessageQueueView(String topicName) throws Exception {
        return getCacheMessageQueueWrapper(this.topicCache, topicName);
    }

    public abstract MessageQueueView getCurrentMessageQueueView(String topicName) throws Exception;

    public abstract ProxyTopicRouteData getTopicRouteForProxy(List<Address> requestHostAndPortList,
        String topicName) throws Exception;

    public abstract String getBrokerAddr(String brokerName) throws Exception;

    public abstract AddressableMessageQueue buildAddressableMessageQueue(MessageQueue messageQueue) throws Exception;

    protected static MessageQueueView getCacheMessageQueueWrapper(LoadingCache<String, MessageQueueView> topicCache,
        String key) throws Exception {
        MessageQueueView res = topicCache.get(key);
        if (res != null && res.isEmptyCachedQueue()) {
            throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST,
                "No topic route info in name server for the topic: " + key);
        }
        return res;
    }

    protected static boolean isTopicRouteValid(TopicRouteData routeData) {
        return routeData != null && routeData.getQueueDatas() != null && !routeData.getQueueDatas().isEmpty()
            && routeData.getBrokerDatas() != null && !routeData.getBrokerDatas().isEmpty();
    }

    protected abstract class AbstractTopicRouteCacheLoader extends AbstractCacheLoader<String, MessageQueueView> {

        public AbstractTopicRouteCacheLoader() {
            super(cacheRefreshExecutor);
        }

        protected abstract TopicRouteData loadTopicRouteData(String topic) throws Exception;

        @Override
        public MessageQueueView getDirectly(String topic) throws Exception {
            try {
                TopicRouteData topicRouteData = loadTopicRouteData(topic);

                if (isTopicRouteValid(topicRouteData)) {
                    MessageQueueView tmp = new MessageQueueView(topic, topicRouteData);
                    log.info("load topic route from namesrv. topic: {}, queue: {}", topic, tmp);
                    return tmp;
                }
                return MessageQueueView.WRAPPED_EMPTY_QUEUE;
            } catch (Exception e) {
                if (TopicRouteHelper.isTopicNotExistError(e)) {
                    return MessageQueueView.WRAPPED_EMPTY_QUEUE;
                }
                throw e;
            }
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load topic route from namesrv failed. topic:{}", key, e);
        }
    }

    protected class TopicRouteCacheLoader extends AbstractTopicRouteCacheLoader {

        @Override
        protected TopicRouteData loadTopicRouteData(String topic) throws Exception {
            return mqClientAPIFactory.getClient().getTopicRouteInfoFromNameServer(topic, Duration.ofSeconds(3).toMillis());
        }
    }
}
