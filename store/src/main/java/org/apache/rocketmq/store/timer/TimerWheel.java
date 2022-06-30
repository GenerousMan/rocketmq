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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TimerWheel {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final int BLANK = -1, IGNORE = -2;

    public ConcurrentHashMap<Long/* delayTime */, Long/* maxOffset */> slotMaxOffsetTable;
    public final int slotsTotal;
    public final int precisionMs;
    public TimerWheel nextWheel = null;
    private String fileName;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final ByteBuffer byteBuffer;
    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return byteBuffer.duplicate();
        }
    };
    private final int wheelLength;
    private final MessageStoreConfig storeConfig;
    public TimerWheel(MessageStoreConfig storeConfig, String fileName, int slotsTotal, int precisionMs) throws IOException {
        this.slotsTotal = slotsTotal;
        this.precisionMs = precisionMs;
        this.fileName = fileName;
        this.storeConfig = storeConfig;
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;
        this.slotMaxOffsetTable = new ConcurrentHashMap<>();
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());

        try {
            randomAccessFile = new RandomAccessFile(this.fileName, "rw");
            if (file.exists() && randomAccessFile.length() != 0 &&
                randomAccessFile.length() != wheelLength) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s",
                    randomAccessFile.length(), wheelLength));
            }
            randomAccessFile.setLength(this.slotsTotal * 2 * Slot.SIZE);
            fileChannel = randomAccessFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
            assert wheelLength == mappedByteBuffer.remaining();
            this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);
            this.byteBuffer.put(mappedByteBuffer);
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean flush) {
        if (flush)
            this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);
        MappedFile.clean(this.byteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Shutdown error in timer wheel", e);
        }
    }

    public void flush() {
        ByteBuffer bf = localBuffer.get();
        bf.position(0);
        bf.limit(wheelLength);
        mappedByteBuffer.position(0);
        mappedByteBuffer.limit(wheelLength);
        for (int i = 0; i < wheelLength; i++) {
            if (bf.get(i) != mappedByteBuffer.get(i)) {
                mappedByteBuffer.put(i, bf.get(i));
            }
        }
        this.mappedByteBuffer.force();
    }

    public Slot getSlot(long timeMs){
        if(timeMs-System.currentTimeMillis()> slotsTotal*precisionMs){
            if(nextWheel!=null) {
                return nextWheel.getSlot(timeMs);
            }
            return null;
        }
        Slot slot = getRawSlot(timeMs);
        if (slot.timeMs != timeMs / precisionMs * precisionMs) {
            return new Slot(timeMs / precisionMs * precisionMs, 0, 0);
        }
        return slot;
    }
    //testable
    public Slot getRawSlot(long timeMs) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        return new Slot(localBuffer.get().getLong() * precisionMs, localBuffer.get().getInt(), localBuffer.get().getInt());
    }

    public int getSlotIndex(long timeMs) {
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    public void Tick(long timeMs){
        // 逐层向上找当前时间的slot，如果有，则无条件转发到本层的所有slot中。
        if(this.nextWheel!=null){
            // 先递归把最上层的转发
            nextWheel.Tick(timeMs);
            Slot slotCheck = nextWheel.getSlot(timeMs);
            Long maxOffset = nextWheel.slotMaxOffsetTable.get(slotCheck.timeMs);
            if(maxOffset!=null){
                System.out.printf("StartDispatch.\n");
                dispatchSlotMessage(slotCheck);
            }
        }
    }

    public void PutMessage(MessageExt msg) throws Exception {
        long timeMs = Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS));
        long diffTime = timeMs-System.currentTimeMillis();
        if(diffTime> slotsTotal*precisionMs){
            if(nextWheel==null) {
                createNextWheel();
            }
            nextWheel.PutMessage(msg);
        }
        else {
            Slot slot = getSlot(timeMs);

            Long maxOffset = this.slotMaxOffsetTable.get(slot.timeMs);
            if(maxOffset==null){
                System.out.printf("no such offset:%d%n",slot.timeMs);
                slotMaxOffsetTable.put(slot.timeMs, 0L);
                maxOffset = 0L;
            }
            slot.setMaxFlushedWhere(maxOffset);
            long flushedOffsetBefore = slot.slotLog.mappedFileQueue.getFlushedWhere();
            slot.putMessage(msg);
            long flushedOffset = slot.slotLog.mappedFileQueue.getFlushedWhere();
            System.out.printf("precision:%d,flushed before:%d, flushedwhere:%d%n",precisionMs,flushedOffsetBefore,flushedOffset);
            this.slotMaxOffsetTable.put(slot.timeMs,flushedOffset);

            putSlot(slot.timeMs,slot.num+1,slot.magic);
        }
    }

    public void dispatchSlotMessage(Slot slotDispatched){
        long slotTimeMs = slotDispatched.timeMs;

        while(true){
            MessageExt msgDispatched = slotDispatched.getNextMessage();
            if(msgDispatched==null){
                break;
            }
            long delayedTime = Long.parseLong(msgDispatched.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS));
            Slot nowSlot = this.getSlot(delayedTime);
            try {
                // 转发一条就更新一次maxOffsetTable
                Long nowSlotOffset = slotMaxOffsetTable.get(nowSlot.timeMs);
                if(nowSlotOffset==null){
                    slotMaxOffsetTable.put(nowSlot.timeMs,0L);
                }
                nowSlot.putMessage(msgDispatched);
                slotMaxOffsetTable.replace(nowSlot.timeMs,nowSlot.slotLog.mappedFileQueue.getFlushedWhere());

                putSlot(nowSlot.timeMs,nowSlot.num+1,nowSlot.magic);
            } catch (Exception e){
                System.out.printf("dispatch fail! now precision:%d, next precision: %d, now slotTime:%d, msgTime:%d. \n", this.precisionMs,nextWheel.precisionMs, nowSlot.timeMs, delayedTime);
            }
        }
        nextWheel.slotMaxOffsetTable.remove(slotTimeMs);

    }

    public static String getTimerWheelPath(final String rootDir, final long precision) {
        return rootDir + File.separator + "timerwheel" + File.separator + precision;
    }
    private boolean createNextWheel(){
        try {
            this.nextWheel = new TimerWheel(storeConfig, getTimerWheelPath(storeConfig.getStorePathRootDir(),slotsTotal * precisionMs), slotsTotal, slotsTotal * precisionMs);
            return true;
        } catch (IOException e){
            System.out.printf("Create next wheel fail.");
            return false;
        }
    }


    public boolean putSlot(long timeMs, int num, int magic) {
        if(timeMs-System.currentTimeMillis()> slotsTotal*precisionMs){
            if(this.nextWheel==null) {
                if(createNextWheel()) {
                    this.nextWheel.putSlot(timeMs, num, magic);
                }
                else{
                    System.out.printf("put slot failed.\n");
                    return false;
                }
            }
            this.nextWheel.putSlot(timeMs, num, magic);
        }
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putInt(num);
        localBuffer.get().putInt(magic);
        return true;
    }

    public void putSlot(long timeMs, long firstPos, long lastPos) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // To be compatible with previous version.
        // The previous version's precision is fixed at 1000ms and it store timeMs / 1000 in slot.
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
    }
    public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        localBuffer.get().putInt(num);
        localBuffer.get().putInt(magic);
    }

    public void reviseSlot(long timeMs, long firstPos, long lastPos, boolean force) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);

        if (timeMs / precisionMs != localBuffer.get().getLong()) {
            if (force) {
                putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
        } else {
            if (IGNORE != firstPos) {
                localBuffer.get().putLong(firstPos);
            } else {
                localBuffer.get().getLong();
            }
            if (IGNORE != lastPos) {
                localBuffer.get().putLong(lastPos);
            }
        }
    }

    //check the timerwheel to see if its stored offset > maxOffset in timerlog
    public long checkPhyPos(long timeStartMs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }
            long first = localBuffer.get().getLong();
            long last = localBuffer.get().getLong();
            if (last > maxOffset) {
                if (first < minFirst) {
                    minFirst = first;
                }
            }
        }
        return minFirst;
    }

    public long getNum(long timeMs) {
        return getSlot(timeMs).num;
    }

    public long getAllNum(long timeStartMs) {
        int allNum = 0;
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs == localBuffer.get().getLong()) {
                localBuffer.get().getLong(); //first pos
                localBuffer.get().getLong(); //last pos
                allNum = allNum + localBuffer.get().getInt();
            }
        }
        return allNum;
    }
}
