package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.store.AllocateMappedFileService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class SlotLogTest {
    private final byte[] msgBody = new byte[1024];
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private String baseDir;
    private SlotLog slotLog;
    private MessageStoreConfig storeConfig;
    private final AtomicInteger counter = new AtomicInteger(0);

    @Before
    public void init() throws IOException {
        baseDir = StoreTestUtils.createBaseDir();
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        storeConfig = new MessageStoreConfig();
        slotLog = new SlotLog(100,storeConfig,null);
    }

    @Test
    public void testCreateSlotLog() {
        Assert.assertEquals(slotLog.mappedFileQueue.getMappedFiles().size(),0);
    }

    @Test
    public void testPutNullMessage() throws Exception {
        long result = slotLog.putMessage(null,0);
        Assert.assertEquals(result,0);
    }

    @Test
    public void testPutMessage() throws Exception {
        // here msg size will be 1204
        MessageExt msgTest = buildMessage(100,"Test", true);
        long putOffset = slotLog.putMessage(msgTest,0);
        Assert.assertEquals(putOffset,1228L);
        Assert.assertEquals(slotLog.mappedFileQueue.getMaxOffset(),1228);

        ByteBuffer msgSize = ByteBuffer.wrap(new byte[8]);
        msgSize.position(0);
        msgSize.limit(4);
        boolean getSizeResult = slotLog.mappedFileQueue.findMappedFileByOffset(0).getData(0,4, msgSize);
        int size = msgSize.getInt(0);
        Assert.assertEquals(size,1228);

        ByteBuffer msgBody = ByteBuffer.wrap(new byte[4096]);
        msgBody.position(0);
        msgBody.limit(size);
        boolean getBodyResult = slotLog.mappedFileQueue.findMappedFileByOffset(0).getData(0,size, msgBody);
        msgBody.position(0);
        MessageExt msgExt = MessageDecoder.decode(msgBody);
        Assert.assertEquals(msgExt.getTopic(),"Test");
    }

    @Test
    public void testSlotInside() throws Exception {
        ByteBuffer msgSize = ByteBuffer.wrap(new byte[4096]);
        MessageExt msgTest = buildMessage(100, "Test", true);
        long result = slotLog.putMessage(msgTest,0);
        result = slotLog.putMessage(msgTest,2405);
        MessageExt msg = slotLog.lookMessageByOffset(0);
        Assert.assertEquals(msg.getTopic(),"Test");

        msg = slotLog.lookMessageByOffset(2405);
        Assert.assertEquals(msg.getTopic(),"Test");

    }

    @Test
    public void testGetMessage() throws Exception {
        long tempOffset = 0;
        for(int i = 0; i < 100; i++) {
            MessageExt msgTest = buildMessage(100, "Test", true);
            long result = slotLog.putMessage(msgTest,tempOffset);
            MessageExt getMsg = slotLog.lookMessageByOffset(tempOffset);
            Assert.assertEquals(getMsg.getTopic(), "Test");
            tempOffset=result;
        }
    }

    @Test
    public void testRecoverSlotLog() throws Exception {
        MessageExt msgTest = buildMessage(1000580, "Test", true);
        long result = slotLog.putMessage(msgTest,0);
        result = slotLog.putMessage(msgTest,result);
        result = slotLog.putMessage(msgTest,result);
        int fileNumOri = slotLog.mappedFileQueue.getMappedFiles().size();

        SlotLog newlog = new SlotLog(100,storeConfig,null);
        newlog.load();
        newlog.recoverOffset();

        int fileNumNew = newlog.mappedFileQueue.getMappedFiles().size();
        Assert.assertEquals(fileNumNew,fileNumOri);
        Assert.assertEquals(slotLog.slotMaxOffset,newlog.slotMaxOffset);
    }

    public MessageExt buildMessage(long delayedMs, String topic, boolean relative) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setQueueId(0);
        msg.setTags(counter.incrementAndGet() + "");
        msg.setKeys("timer");
        if (relative) {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_SEC, delayedMs / 1000 + "");
        } else {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_DELIVER_MS, delayedMs + "");
        }
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
}
