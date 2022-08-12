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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TimerWheelTest {

    private String baseDir;
    private final byte[] msgBody = new byte[1024];
    private SocketAddress bornHost;
    private SocketAddress storeHost;

    private final int slotsTotal = 60;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int precisionMs = 1000;
    private TimerWheel timerWheel;

    private final long defaultDelay = System.currentTimeMillis() / precisionMs * precisionMs;

    @Before
    public void init() throws IOException {
        baseDir = StoreTestUtils.createBaseDir();
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        timerWheel = new TimerWheel(new MessageStoreConfig(),baseDir, slotsTotal, precisionMs);
    }

    @Test
    public void testPutMsg() throws Exception {
        int precision = 1;
        int slotNum = 20;
        long delayTime = defaultDelay + precision*slotNum/2;
        MessageExt testMsg = buildMessage(delayTime,"Test", false);
        baseDir = StoreTestUtils.createBaseDir();
        timerWheel = new TimerWheel(new MessageStoreConfig(),baseDir, slotNum, precision);
        timerWheel.PutMessage(testMsg);

        Slot slotGetBack = timerWheel.getSlot(delayTime);
        MessageExt msgGetBack = slotGetBack.getNextMessage(0);
        Assert.assertNotEquals(msgGetBack,null);
        Assert.assertEquals(msgGetBack.getTopic(),"Test");
    }


    @Test
    public void testMultiTick() throws Exception {
        int slotNum = 20;
        int precision = 1;int precision2 = 20;int precision3 = 400;

        long curTime = System.currentTimeMillis();
        baseDir = StoreTestUtils.createBaseDir();
        timerWheel = new TimerWheel(new MessageStoreConfig(),baseDir, slotNum, precision);
        Assert.assertEquals(timerWheel.nextWheel, null);

        // 设置一条消息，在第三层的第二格中， 此处延迟为80秒
        long delayTime = curTime/precision*precision + (long)(precision3*2);
        MessageExt testMsg = buildMessage(delayTime,"Test", false);
        timerWheel.PutMessage(testMsg);

        Assert.assertNotEquals(timerWheel.nextWheel,null);
        Assert.assertNotEquals(timerWheel.nextWheel.nextWheel,null);

        // 给第三层转3格的时间，能将消息逐层转发至最下层时间轮中。
        for(int i = 0; i < precision*slotNum*slotNum*3; i++) {
            Thread.sleep(precision);
            timerWheel.Tick(curTime+i*precision);
        }

        Assert.assertNotEquals(timerWheel.slotMaxOffsetTable.size(),0);

    }

    @Test
    public void testDeleteOffsetTable(){
    // TODO 检查是否能在消息转发至下层后，删除当前的offsetTable。

    }

    @Test
    public void testTick() throws Exception {
        int precision = 100;
        int slotNum = 20;
        int upPrecision = precision*slotNum;
        long curTime = System.currentTimeMillis();
        // 这里必须比原本的slotNum多至少一个单位时间，否则还是会被定位到第一层的最后一个slot中
        baseDir = StoreTestUtils.createBaseDir();
        timerWheel = new TimerWheel(new MessageStoreConfig(),baseDir, slotNum, precision);
        Assert.assertEquals(timerWheel.nextWheel, null);

        for(int i=0;i<5;i++){
            long delayTime = curTime/precision*precision + (long)(precision*(slotNum+1+i));
            MessageExt testMsg = buildMessage(delayTime,"Test", false);
            timerWheel.PutMessage(testMsg);
        }

        // 原本的延迟时间为1圈多1格，现在转2圈，应当能在这个过程中把消息加入到前一层。
        for(int i = 0; i < 2*slotNum; i++) {
            Thread.sleep(precision);
            timerWheel.Tick(curTime+i*precision);
        }

        // 消息传回第一个wheel内。
        Assert.assertEquals(timerWheel.slotMaxOffsetTable.size(),5);
    }

    @Test
    public void testAutoCreateOverflowWheel() throws IOException {
        baseDir = StoreTestUtils.createBaseDir();
        timerWheel = new TimerWheel(new MessageStoreConfig(),baseDir, slotsTotal, precisionMs);
        long delayedTime = defaultDelay + (slotsTotal/2)*precisionMs;
        long overflowDelayedTime = defaultDelay + (slotsTotal*2)*precisionMs;
        timerWheel.putSlot(delayedTime,2,5);
        Assert.assertEquals(timerWheel.nextWheel,null);
        timerWheel.putSlot(overflowDelayedTime,2,5);
        Assert.assertNotEquals(timerWheel.nextWheel,null);
    }



    public MessageExt buildMessage(long delayedMs, String topic, boolean relative) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setQueueId(0);
        msg.setTags(counter.incrementAndGet() + "");
        msg.setKeys("timer");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_OUT_MS, delayedMs + "");
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(4);
        msg.setBornTimestamp(1234);
        msg.setBornHost(bornHost);
        msg.setStoreHost(storeHost);
        MessageClientIDSetter.setUniqID(msg);
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msg.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msg.getTags());
        msg.setTagsCode(tagsCodeValue);

        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }
    @After
    public void shutdown() {
        if (null != timerWheel) {
            timerWheel.shutdown();
        }
        if (null != baseDir) {
            StoreTestUtils.deleteFile(baseDir);
        }
    }


}
