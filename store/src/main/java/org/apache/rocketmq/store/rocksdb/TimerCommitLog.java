package org.apache.rocketmq.store.rocksdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


import static org.apache.rocketmq.common.message.MessageDecoder.decodeMessage;

public class TimerCommitLog extends AbstractRocksDBStorage {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ScanExpireService scanExpireService;
    private final DispatchExpireService[] dispatchExpireServices;
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    private static final byte CTRL_A = '\u0001';
    private final int kvCommitlogKeyLen = 19 + 32 + 32 + 2;
    private final int kvCommitlogValueLen;
    private final DefaultMessageStore messageStore;
    private final BlockingQueue<MessageExt> putBackQueue;

    public TimerCommitLog(DefaultMessageStore messageStore, final String dbPath) throws RocksDBException {
        this.dbPath = dbPath;
        this.messageStore = messageStore;

        this.kvCommitlogValueLen = 4096;

        this.putBackQueue = new LinkedBlockingQueue<MessageExt>(10240);

        postLoad();

        this.scanExpireService = new ScanExpireService();
        this.scanExpireService.start();

        this.dispatchExpireServices = new DispatchExpireService[3];
        for (int i = 0; i < 3; i++) {
            this.dispatchExpireServices[i] = new DispatchExpireService();
            this.dispatchExpireServices[i].start();
        }
    }

    class DispatchExpireService extends StateService {
        @Override
        public void run() {
            System.out.printf(getServiceName() + " service start\n");
            log.info(getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (putBackQueue.size() == 0) {
                        waitForRunning(100);
                        continue;
                    }
                    MessageExt msg = putBackQueue.poll(10, TimeUnit.MILLISECONDS);
                    if(msg!=null) {
                        MessageExtBrokerInner msgInner = convertMessage(msg);
                        PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
                        System.out.printf("Dispatch finish: %s ; append: %s %n", putMessageResult.getPutMessageStatus(),putMessageResult.getAppendMessageResult());
                    }
                } catch (Throwable e) {
                    System.out.printf("Error occurred in " + getServiceName() + e);
                    log.error("Error occurred in " + getServiceName(), e);
                }
            }
            System.out.printf(getServiceName() + " service end\n");
            log.info(getServiceName() + " service end");
        }
    }

    class ScanExpireService extends StateService {
        @Override
        public void run() {
            System.out.printf(getServiceName() + " service start\n");
            log.info(getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    RocksIterator iter = db.newIterator(defaultCFHandle);
                    int deleteCount = 0;

                    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                        long s = parse19ByteArrayToLong(iter.key());
                        if (s > System.currentTimeMillis()) {
                            // 预读下一秒数据
                            if(s>System.currentTimeMillis()+1000) {
                                break;
                            }
                            continue;
                        }
                        byte[] value = iter.value();
                        // 解码value
                        MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(value));
                        // 放进blocking queue
                        putBackQueue.put(msg);
                        delete(iter.key());
                    }
                    // System.out.printf(getServiceName() + " scan finish...\n");
                    waitForRunning(100);
                } catch (Throwable e) {
                    System.out.printf("Error occurred in " + getServiceName() + e);
                    log.error("Error occurred in " + getServiceName(), e);
                }
            }
            System.out.printf(getServiceName() + " service end\n");
            log.info(getServiceName() + " service end");
        }
    }

    private void initOptions() {
        this.options = RocksDBOptionsFactory.createDBOptions();
        //RocksDBOptionsFactory.initRocksDBConfig(null, env, this.options, null);

        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);
        this.writeOptions.setDisableWAL(true);
        this.writeOptions.setNoSlowdown(true);

        this.ableWalWriteOptions = new WriteOptions();
        this.ableWalWriteOptions.setSync(false);
        this.ableWalWriteOptions.setDisableWAL(false);
        this.ableWalWriteOptions.setNoSlowdown(true);

        this.readOptions = new ReadOptions();
        this.readOptions.setPrefixSameAsStart(true);
        this.readOptions.setTotalOrderSeek(false);
        this.readOptions.setTailing(false);

        this.totalOrderReadOptions = new ReadOptions();
        this.totalOrderReadOptions.setPrefixSameAsStart(false);
        this.totalOrderReadOptions.setTotalOrderSeek(false);

        this.compactRangeOptions = new CompactRangeOptions();
        this.compactRangeOptions.setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce);
        this.compactRangeOptions.setAllowWriteStall(true);
        this.compactRangeOptions.setExclusiveManualCompaction(false);
        this.compactRangeOptions.setChangeLevel(true);
        this.compactRangeOptions.setTargetLevel(-1);
        this.compactRangeOptions.setMaxSubcompactions(4);

        this.compactionOptions = new CompactionOptions();
        this.compactionOptions.setCompression(CompressionType.LZ4_COMPRESSION);
        this.compactionOptions.setMaxSubcompactions(4);
        this.compactionOptions.setOutputFileSizeLimit(4 * 1024 * 1024 * 1024L);
    }

    // TODO: 后续可以考虑增加options的可选内容，目前POC验证不作考虑。
    @Override
    protected boolean postLoad() throws RocksDBException {
        try {
            final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeLevelStyleCompaction().setEnableBlobFiles(true).setMinBlobSize(0);
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
            final List<ColumnFamilyHandle> cfHandles = new ArrayList();
            initOptions();
            //this.options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            open(cfDescriptors, cfHandles);
            this.defaultCFHandle = cfHandles.get(0);
            return true;
        } catch (final Exception e) {
            log.error("postLoad Failed. {}", this.dbPath, e);
            return false;
        }
    }

    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        long delayTime = msg.getDelayTime();
        String topic = msg.getTopic();
        String msgid = msg.getMsgId();
        AppendMessageResult result = null;

        try {
            byte[] key = buildKvCommitlogKey(delayTime, topic, msgid);
            byte[] value = MessageDecoder.encode(msg, false);
            put(key, 19 + 32 + 32 + 2, value);
            PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK);
            // System.out.printf("now timer message put successful,topic:" + topic + ", msgid:" + msgid + ", key:" + key+ "\n value:" + value + "\n");
            return CompletableFuture.completedFuture(putMessageResult);

            // return putMessageResult;
        } catch (Exception e) {
            System.out.printf("error：:" + e + "\n");
            PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR);
            return CompletableFuture.completedFuture(putMessageResult);
            //PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR);
            //return putMessageResult;
        }

    }

    abstract class StateService extends ServiceThread {
        public static final int INITIAL = -1, START = 0, WAITING = 1, RUNNING = 2, END = 3;
        protected int state = INITIAL;

        protected void setState(int state) {
            this.state = state;
        }

        protected boolean isState(int state) {
            return this.state == state;
        }

        @Override
        public String getServiceName() {
            return this.getClass().getSimpleName();
        }
    }

    @Override
    protected void preShutdown() {
    }

    public void put(final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws Exception {
        put(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes, keyLen, valueBytes, valueBytes.length);
    }

    public void put(final ByteBuffer keyBB, final ByteBuffer valueBB) throws Exception {
        put(this.defaultCFHandle, this.ableWalWriteOptions, keyBB, valueBB);
    }

    public byte[] get(final byte[] keyBytes) throws Exception {
        return get(this.defaultCFHandle, this.totalOrderReadOptions, keyBytes);
    }

    public boolean get(final ByteBuffer keyBB, final ByteBuffer valueBB) throws Exception {
        return get(this.defaultCFHandle, this.totalOrderReadOptions, keyBB, valueBB);
    }

    public void delete(final byte[] keyBytes) throws Exception{
        delete(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes);
    }

    public void batchPutWithWal(final WriteBatch batch) throws RocksDBException {
        batchPut(this.ableWalWriteOptions, batch);
    }

    public void rangeDelete(final byte[] startKey, final byte[] endKey) throws RocksDBException {
        rangeDelete(this.defaultCFHandle, this.ableWalWriteOptions, startKey, endKey);
    }

    public RocksIterator iterator() {
        return this.db.newIterator(this.defaultCFHandle, this.totalOrderReadOptions);
    }

    private void buildKvCommitlogKey(final ByteBuffer keyBB, long delayedTime, String topic, String msgid) {
        keyBB.position(0).limit(kvCommitlogKeyLen);
        if (msgid == null) {
            msgid = createRandomStr(16);
        }
        keyBB.put(longTo19ByteArray(delayedTime)).put(CTRL_A).
                put(getBytes(topic, 32)).put(CTRL_A).
                put(getBytes(msgid, 32));
        keyBB.flip();
    }

    private byte[] buildKvCommitlogKey(long delayedTime, String topic, String msgid) {
        if (msgid == null) {
            msgid = createRandomStr(16);
        }
        byte[] key = new byte[kvCommitlogKeyLen];
        byte[] delayBytes = longTo19ByteArray(delayedTime);
        byte[] topicBytes = getBytes(topic, 32);
        byte[] msgidBytes = getBytes(msgid, 32);
        System.arraycopy(delayBytes, 0, key, 0, delayBytes.length);
        key[delayBytes.length] = 0x00;
        System.arraycopy(topicBytes, 0, key, delayBytes.length + 1, topicBytes.length);
        key[delayBytes.length + topicBytes.length] = 0x00;
        System.arraycopy(msgidBytes, 0, key, delayBytes.length + topicBytes.length + 2, msgidBytes.length);
        return key;
    }

    private static String createRandomStr(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }

    private static byte[] longTo19ByteArray(final long x) {
        String s = String.format("%019d", x);
        return s.getBytes(CHARSET_UTF8);
    }

    private static Long parse19ByteArrayToLong(final byte[] bytes) {
        if (bytes == null || bytes.length < 19) {
            return null;
        }
        String s = new String(bytes, 0, 19, CHARSET_UTF8);
        return Long.parseLong(s);
    }


    private byte[] getBytes(String s, int length) {
        byte[] oriBytes = s.getBytes(CHARSET_UTF8);
        byte[] returnBytes = new byte[length];
        if (oriBytes.length < length) {
            int fixLength = length - oriBytes.length;
            System.arraycopy(oriBytes, 0, returnBytes, 0, oriBytes.length);
            for (int x = length - fixLength; x < length; x++) {
                returnBytes[x] = 0x00;
            }
        }
        // 长度大于限制长度，取前面限制长度的字符，后面不取。
        else {
            System.arraycopy(oriBytes, 0, returnBytes, 0, length);
        }
        return returnBytes;
    }

    private MessageExtBrokerInner convertMessage(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setDelayTime(0);

        msgInner.setWaitStoreMsgOK(false);

        return msgInner;
    }

}
