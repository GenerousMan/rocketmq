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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class TimerProducer {

    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT_SEC = 10000;
    public static final int TOTAL_TIME_SEC = 3600;
    public static final int START_TIME_SEC = 3700;
    public static final int END_TIME_SEC = START_TIME_SEC+TOTAL_TIME_SEC;
    public static final int MESSAGE_COUNT = MESSAGE_COUNT_SEC*TOTAL_TIME_SEC;

    public static final int THREAD_COUNT = 8;
    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "Delay_BenchmarkTest";
    public static final String TAG = "TagA";
    private static final ExecutorService sendThreadPool = new ThreadPoolExecutor(
            THREAD_COUNT,
            THREAD_COUNT,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("ProducerSendMessageThread_"));

    public static volatile int totalNum = 0;
    public static long timeStamp = System.currentTimeMillis();
    public static long startTimeStamp = System.currentTimeMillis();

    public static void main(String[] args) throws MQClientException, InterruptedException {
        /*
         * Instantiate with a producer group name.
         */


        for (int i = 0; i < THREAD_COUNT; i++) {
            int finalI = i;
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP+ finalI);
                        producer.setNamesrvAddr("127.0.0.1:9876");
                        producer.start();

                        for (int j = 0; j < MESSAGE_COUNT / THREAD_COUNT; j++) {
                            Message msg = new Message(TOPIC /* Topic */,
                                    TAG /* Tag */,
                                    ("Hello RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                            );
                            long randomDelay = startTimeStamp + (long)(Math.random() * (END_TIME_SEC-START_TIME_SEC+1)*1000);
                            msg.setDeliverTimeMs(randomDelay);
                            /*
                             * Call send message to deliver message to one of brokers.
                             */
                            SendResult sendResult = producer.send(msg);
                            totalNum += 1;
                            if (totalNum % 10000 == 0) {
                                long newTime = System.currentTimeMillis();
                                System.out.printf("10000 send finished, cost time :%d.%n", newTime - timeStamp);
                                timeStamp = newTime;
                                totalNum = 0;
                            }
                        }
                        producer.shutdown();
                    } catch (Exception e) {
                        System.out.printf("Something wrong.\n"+e);
                    }

                    // System.out.printf("%s%n", sendResult);
                }

            });
        }

    }
}
