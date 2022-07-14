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
    public long slotMaxOffset;
    public final MessageStoreConfig storeConfig;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    public final long timeMs;

    public SlotLog(final long timeMs, final MessageStoreConfig storeConfig, final AllocateMappedFileService allocateMappedFileService) {
        this.timeMs = timeMs;
        this.storeConfig = storeConfig;
        String storePath = storeConfig.getStorePathSlotLog(-1, timeMs);
        this.slotMaxOffset = 0;
        this.mappedFileQueue = new MappedFileQueue(storePath,
                storeConfig.getMappedFileSizeSlotLog(),
                allocateMappedFileService);
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(storeConfig.getMaxMessageSize());
            }
        };

    }

    public SlotLog(final int precision, final long timeMs, final MessageStoreConfig storeConfig, final AllocateMappedFileService allocateMappedFileService) {
        this.timeMs = timeMs;
        this.storeConfig = storeConfig;
        String storePath = storeConfig.getStorePathSlotLog(precision, timeMs);
        this.slotMaxOffset = 0;
        this.mappedFileQueue = new MappedFileQueue(storePath,
                storeConfig.getMappedFileSizeSlotLog(),
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

    public void recoverOffset(){
        long maxOffset = 0;
        while(true){
            long newOffset = getNextMessageOffset(maxOffset);
            if(newOffset!=-1){
                maxOffset = newOffset;
            }
            else{
                break;
            }
        }
        this.slotMaxOffset = maxOffset;
    }

    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = storeConfig.getMappedFileSizeSlotLog();
        byteBuffer.position(0);
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
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

    public long putMessage(final MessageExt msg, long putOffset) throws Exception {
        if(msg==null){
            return putOffset;
        }

        byte[] encodeMsg = MessageDecoder.encode(msg,false);

        long msgFileOffset = 0;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        long totalPutOffset = (long)(this.mappedFileQueue.getMappedFiles().size()) * (long)storeConfig.getMappedFileSizeSlotLog();
        if(mappedFile==null || putOffset+encodeMsg.length >= totalPutOffset){
            if(mappedFile!=null){
                mappedFile.setWrotePosition(mappedFile.getFileSize());
            }
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            msgFileOffset = 0;
            putOffset = (long)(this.mappedFileQueue.getMappedFiles().size()-1) * (long)mappedFile.getFileSize();
        } else{
            msgFileOffset = (putOffset - (long)(this.mappedFileQueue.getMappedFiles().size()-1) * (long)mappedFile.getFileSize());
        }
        mappedFile.setWrotePosition((int)msgFileOffset);
        try {
            mappedFile.appendMessage(encodeMsg,0,encodeMsg.length);
            this.slotMaxOffset = putOffset+encodeMsg.length;
            // System.out.printf("After put offset:%d.%n",slotMaxOffset);
            return slotMaxOffset;
        } catch (Exception e){
            System.out.printf("Put to slot error:"+e+"\n");
            return putOffset;
        }
    }

    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = storeConfig.getMappedFileSizeSlotLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public MessageExt lookMessageByOffset(long slotLogOffset, int size) {
        SelectMappedBufferResult sbr = getMessage(slotLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }
        return null;
    }

    public MessageExt lookMessageByOffset(long slotLogOffset) {
        SelectMappedBufferResult sbr = getMessage(slotLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(slotLogOffset, size);
            } finally {
                sbr.release();
            }
        }
        return null;
    }

    // TODO: consider more, such as if the broker shutdown, how to recover the readWhere pointer.
    public long getNextMessageOffset(long offset){
        ByteBuffer msgSize = ByteBuffer.wrap(new byte[4]);
        msgSize.position(0);
        msgSize.limit(4);
        try {
            getData(offset, 4, msgSize);
            int size = msgSize.getInt(0);
            if (size > 0 && size < storeConfig.getMaxMessageSize()) {
                return offset + size;
            } else {
                return -1;
            }
        } catch(Exception e){
            return -1;
        }
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
