package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class SlotLog {
    protected final MappedFileQueue mappedFileQueue;
    public final MessageStoreConfig storeConfig;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    public final long timeMs;

    public SlotLog(final long timeMs, final MessageStoreConfig storeConfig, final AllocateMappedFileService allocateMappedFileService) {
        this.timeMs = timeMs;
        this.storeConfig = storeConfig;
        String storePath = storeConfig.getStorePathSlotLog(timeMs);
        this.mappedFileQueue = new MappedFileQueue(storePath,
                storeConfig.getMappedFileSizeCommitLog(),
                allocateMappedFileService);
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(storeConfig.getMaxMessageSize());
            }
        };
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public void load(){
        this.mappedFileQueue.load();
    }

    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = storeConfig.getMappedFileSizeSlotLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }
    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    public byte[] byteMerger(byte[] byte_1, byte[] byte_2){
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        return byte_3;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public boolean putMessage(final MessageExt msg) throws Exception {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        if(mappedFile==null || mappedFile.isFull()){
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        if(msg==null){
            return false;
        }
        byte[] encodeMsg = MessageDecoder.encode(msg,false);
        int msgLength = encodeMsg.length;
        byte[] mergeBuffer = byteMerger(intToByteArray(msgLength),encodeMsg);
        try {
            System.out.printf("flushed:%d%n",this.mappedFileQueue.getFlushedWhere());
            mappedFile.appendMessage(mergeBuffer,(int)this.mappedFileQueue.getFlushedWhere(),mergeBuffer.length);
            this.flush();
            return true;
        } catch (Exception e){
            return false;
        }
    }

    // TODO: consider more, such as if the broker shutdown, how to recover the readWhere pointer.
    public MessageExt getNextMessage(){
        ByteBuffer msgSize = ByteBuffer.wrap(new byte[8]);
        msgSize.position(0);
        msgSize.limit(4);
        getData(this.mappedFileQueue.readWhere,4,msgSize);
        int size = msgSize.getInt(0);
        ByteBuffer msgBody = ByteBuffer.wrap(new byte[4096]);
        msgBody.position(0);
        msgBody.limit(size);
        getData(this.mappedFileQueue.readWhere+4,size,msgBody);
        msgBody.position(0);
        MessageExt msgExt = MessageDecoder.decode(msgBody);
        this.mappedFileQueue.readWhere += (4+size);
        return msgExt;
    }


    private String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    static class PutMessageThreadLocal {
        private CommitLog.MessageExtEncoder encoder;
        private StringBuilder keyBuilder;

        PutMessageThreadLocal(int maxMessageBodySize) {
            encoder = new CommitLog.MessageExtEncoder(maxMessageBodySize);
            keyBuilder = new StringBuilder();
        }

        public CommitLog.MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }

    private MessageExtBrokerInner convertMessage(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);

        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());
        return msgInner;
    }


}
