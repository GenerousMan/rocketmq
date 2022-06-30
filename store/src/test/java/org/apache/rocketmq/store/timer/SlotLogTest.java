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
        boolean result = slotLog.putMessage(null);
        Assert.assertEquals(result,false);
    }

    @Test
    public void testPutMessage() throws Exception {
        // here msg size will be 1204
        MessageExt msgTest = buildMessage(100,"Test", true);
        boolean result = slotLog.putMessage(msgTest);
        Assert.assertEquals(result,true);
        Assert.assertEquals(slotLog.mappedFileQueue.getMaxOffset(),4+1204);

        ByteBuffer msgSize = ByteBuffer.wrap(new byte[8]);
        msgSize.position(0);
        msgSize.limit(4);
        boolean getSizeResult = slotLog.mappedFileQueue.findMappedFileByOffset(0).getData(0,4, msgSize);
        int size = msgSize.getInt(0);
        Assert.assertEquals(size,1204);

        ByteBuffer msgBody = ByteBuffer.wrap(new byte[4096]);
        msgBody.position(0);
        msgBody.limit(size);
        boolean getBodyResult = slotLog.mappedFileQueue.findMappedFileByOffset(0).getData(4,size, msgBody);
        msgBody.position(0);
        MessageExt msgExt = MessageDecoder.decode(msgBody);
        Assert.assertEquals(msgExt.getTopic(),"Test");
    }

    @Test
    public void testGetMessage() throws Exception {

        for(int i = 0; i < 1000; i++) {
            // here msg size will be 1204
            MessageExt msgTest = buildMessage(100, "Test"+i, true);
            boolean result = slotLog.putMessage(msgTest);
        }
        for(int i = 0; i < 1000; i++) {
            MessageExt getMsg = slotLog.getNextMessage();
            Assert.assertEquals(getMsg.getTopic(), "Test"+i);
        }
    }

    @Test
    public void testRecoverSlotLog() throws Exception {
        MessageExt msgTest = buildMessage(100, "Test", true);
        boolean result = slotLog.putMessage(msgTest);
        MessageExt getMsg = slotLog.getNextMessage();

        int fileNumOri = slotLog.mappedFileQueue.getMappedFiles().size();
        SlotLog newlog = new SlotLog(100,storeConfig,null);
        newlog.load();
        int fileNumNew = newlog.mappedFileQueue.getMappedFiles().size();
        Assert.assertEquals(fileNumNew,fileNumOri);
        Assert.assertEquals(slotLog.mappedFileQueue.getMaxOffset(),newlog.mappedFileQueue.getMaxOffset());
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
