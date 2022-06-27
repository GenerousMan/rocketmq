/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.transaction;

import java.util.List;
import java.util.Random;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AbstractTransactionServiceTest extends InitConfigAndLoggerTest {

    private static final String BROKER_NAME = "mockBorker";
    private static final Random RANDOM = new Random();

    public static class MockAbstractTransactionServiceTest extends AbstractTransactionService {

        @Override
        protected String getBrokerNameByAddr(String brokerAddr) {
            return BROKER_NAME;
        }

        @Override
        public void addTransactionSubscription(String group, List<String> topicList) {

        }

        @Override
        public void addTransactionSubscription(String group, String topic) {

        }

        @Override
        public void replaceTransactionSubscription(String group, List<String> topicList) {

        }

        @Override
        public void unSubscribeAllTransactionTopic(String group) {

        }
    }

    private TransactionService transactionService;

    @Before
    public void before() throws Throwable {
        super.before();
        this.transactionService = new MockAbstractTransactionServiceTest();
    }

    @Test
    public void testAddAndGenEndHeader() {
        Message message = new Message();
        message.putUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS, "30");
        String txId = MessageClientIDSetter.createUniqID();

        TransactionData transactionData = transactionService.addTransactionDataByBrokerName(
            BROKER_NAME,
            RANDOM.nextLong(),
            RANDOM.nextLong(),
            txId,
            message
        );
        assertNotNull(transactionData);

        EndTransactionRequestData requestData = transactionService.genEndTransactionRequestHeader(
            "group",
            MessageSysFlag.TRANSACTION_COMMIT_TYPE,
            true,
            txId,
            txId
        );

        assertEquals(BROKER_NAME, requestData.getBrokerName());
        assertEquals(BROKER_NAME, transactionData.getBrokerName());
        assertEquals(transactionData.getCommitLogOffset(), requestData.getRequestHeader().getCommitLogOffset().longValue());
        assertEquals(transactionData.getTranStateTableOffset(), requestData.getRequestHeader().getTranStateTableOffset().longValue());

        assertNull(transactionService.genEndTransactionRequestHeader(
            "group",
            MessageSysFlag.TRANSACTION_COMMIT_TYPE,
            true,
            txId,
            txId
        ));
    }

    @Test
    public void testOnSendCheckTransactionStateFailedFailed() {
        Message message = new Message();
        message.putUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS, "30");
        String txId = MessageClientIDSetter.createUniqID();

        TransactionData transactionData = transactionService.addTransactionDataByBrokerName(
            BROKER_NAME,
            RANDOM.nextLong(),
            RANDOM.nextLong(),
            txId,
            message
        );
        transactionService.onSendCheckTransactionStateFailed(ProxyContext.createForInner(this.getClass()), transactionData);
        assertNull(transactionService.genEndTransactionRequestHeader(
            "group",
            MessageSysFlag.TRANSACTION_COMMIT_TYPE,
            true,
            txId,
            txId
        ));
    }
}