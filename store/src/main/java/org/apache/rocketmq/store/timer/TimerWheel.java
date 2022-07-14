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
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TimerWheel {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static final int BLANK = -1, IGNORE = -2;

    public ConcurrentHashMap<Long/* delayTime */, Long/* maxOffset */> slotMaxOffsetTable;
    public ConcurrentHashMap<Long/* delayTime */, Long/* readOffset */> slotReadOffsetTable;
    public ConcurrentHashMap<Long/* delayTime */, Slot/* Slot */> slotTable;
    public final int slotsTotal;
    public final int precisionMs;
    public TimerWheel nextWheel = null;
    public TimerWheel beforeWheel = null;
    private String fileName;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final ByteBuffer byteBuffer;
    private final int deleteDelaySlot = 30;
    private final int tickBeforeSlot = 10;

    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return byteBuffer.duplicate();
        }
    };
    private final int wheelLength;
    private final MessageStoreConfig storeConfig;
    Long temp = 0L;
    public TimerWheel(MessageStoreConfig storeConfig, String fileName, int slotsTotal, int precisionMs) throws IOException {
        this.slotsTotal = slotsTotal;
        this.precisionMs = precisionMs;
        this.fileName = fileName;
        this.storeConfig = storeConfig;
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;
        this.slotMaxOffsetTable = new ConcurrentHashMap<>();
        this.slotReadOffsetTable = new ConcurrentHashMap<>();
        this.slotTable = new ConcurrentHashMap<>();
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
        if(this.nextWheel!=null) {
            this.nextWheel.flush();
        }
        for(Slot slot:slotTable.values()){
            slot.slotLog.flush();
        }
    }

    public Slot getSlot(long timeMs){
        if(timeMs-System.currentTimeMillis()> slotsTotal*precisionMs){
            if(nextWheel!=null) {
                return nextWheel.getSlot(timeMs);
            }
            return null;
        }
        Slot slot = slotTable.get(timeMs / precisionMs * precisionMs);
        if(slot==null){
            slot = new Slot(precisionMs, timeMs / precisionMs * precisionMs, 0, 0);
            slotTable.put(slot.timeMs, slot);
        }
        return slot;
    }

    public Slot forceGetSlotHere(long timeMs){
        Slot slot = slotTable.get(timeMs / precisionMs * precisionMs);
        if(slot==null){
            slot = new Slot(precisionMs, timeMs / precisionMs * precisionMs, 0, 0);
            slotTable.put(slot.timeMs, slot);
        }
        return slot;
    }

    //testable
    public Slot getRawSlot(long timeMs) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        return new Slot(precisionMs, localBuffer.get().getLong() * precisionMs, localBuffer.get().getInt(), localBuffer.get().getInt());
    }

    public int getSlotIndex(long timeMs) {
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    public void Tick(long timeMs){
        // 逐层向上找当前时间的slot，如果有，则无条件转发到本层的所有slot中。
        long preTimeMs = preTickTimeMs(timeMs);
        if(this.nextWheel!=null){
            // 先递归把最上层的转发
            nextWheel.Tick(preTimeMs);
            Slot slotCheck = nextWheel.forceGetSlotHere(preTimeMs);
            Long maxOffset = nextWheel.slotMaxOffsetTable.get(slotCheck.timeMs);
            if(maxOffset!=null){
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
            long newMaxOffset = slot.putMessage(msg, maxOffset);
            // System.out.printf("precision:%d,flushed before:%d, flushedwhere:%d%n",precisionMs,maxOffset,newMaxOffset);
            this.slotMaxOffsetTable.replace(slot.timeMs,newMaxOffset);
            this.slotTable.put(slot.timeMs,slot);
            // putSlot(slot.timeMs,slot.num+1,slot.magic);
        }
    }

    public void dispatchSlotMessage(Slot slotDispatched){
        long slotTimeMs = slotDispatched.timeMs;
        // 从上一次转发完的readOffset起始。
        Long tempNextOffset = nextWheel.slotReadOffsetTable.get(slotTimeMs);
        Long maxNextOffset = nextWheel.slotMaxOffsetTable.get(slotTimeMs);
        if(tempNextOffset==null){
            nextWheel.slotReadOffsetTable.put(slotTimeMs,0L);
            tempNextOffset = 0L;
        }
        int count = 0;
        while(tempNextOffset<maxNextOffset){
            MessageExt msgDispatched = slotDispatched.getNextMessage(tempNextOffset);
            if(msgDispatched==null){
                long slotLogSize = storeConfig.getMappedFileSizeSlotLog();
                // 下个文件的起点位置。
                tempNextOffset = tempNextOffset / slotLogSize * slotLogSize + slotLogSize;
                msgDispatched = slotDispatched.getNextMessage(tempNextOffset);
            }
            long delayedTime = Long.parseLong(msgDispatched.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS));
            Slot nowSlot = this.forceGetSlotHere(delayedTime);
            // System.out.printf("Dispatched. Slot %d to Slot %d.\n", slotDispatched.timeMs,nowSlot.timeMs);
            try {
                // 转发一条就更新一次maxOffsetTable
                Long beforeSlotOffset = slotMaxOffsetTable.get(nowSlot.timeMs);
                if(beforeSlotOffset==null){
                    slotMaxOffsetTable.put(nowSlot.timeMs,0L);
                    beforeSlotOffset = 0L;
                }
                long nowSlotOffset = nowSlot.putMessage(msgDispatched, beforeSlotOffset);
                slotMaxOffsetTable.replace(nowSlot.timeMs,nowSlotOffset);
                tempNextOffset+=(nowSlotOffset-beforeSlotOffset);
                nextWheel.slotReadOffsetTable.replace(slotTimeMs,tempNextOffset);
                log.info("["+System.currentTimeMillis()+ "]Slot "+slotTimeMs+" dispatch to Slot" +nowSlot.timeMs+" finished once.now offset:"+tempNextOffset+", total: "+maxNextOffset+"\n");
                putSlot(nowSlot.timeMs,nowSlot.num+1,nowSlot.magic);
            } catch (Exception e){
                System.out.printf("dispatch fail! now precision:%d, next precision: %d, now slotTime:%d, msgTime:%d. \n", this.precisionMs,nextWheel.precisionMs, nowSlot.timeMs, delayedTime);
            }
        }
        // 转发完了，由于转发存在提前量，可能仍然会有一定消息写入，则此处不可以删除slotMaxOffsetTable中的指定项。
        // nextWheel.slotMaxOffsetTable.remove(slotTimeMs);
        putSlot(slotDispatched.timeMs,0,slotDispatched.magic);
    }

    public static String getTimerWheelPath(final String rootDir, final long precision) {
        return rootDir + File.separator + "timerwheel" + File.separator + precision;
    }
    private boolean createNextWheel(){
        try {
            this.nextWheel = new TimerWheel(storeConfig, getTimerWheelPath(storeConfig.getStorePathRootDir(),slotsTotal * precisionMs), slotsTotal, slotsTotal * precisionMs);
            nextWheel.beforeWheel = this;
            return true;
        } catch (IOException e){
            System.out.printf("Create next wheel fail.");
            return false;
        }
    }

    public MessageExt getSlotNextMessage(Slot slot){
        Long nowReadOffset = this.slotReadOffsetTable.get(slot.timeMs);
        Long nowMaxOffset = this.slotMaxOffsetTable.get(slot.timeMs);
        if(nowReadOffset==null){
            this.slotReadOffsetTable.put(slot.timeMs,0L);
            nowReadOffset = 0L;
        }

        if(nowMaxOffset==null || nowReadOffset>=nowMaxOffset){
            return null;
        }
        MessageExt nextMessage = slot.getNextMessage(nowReadOffset);
        this.slotReadOffsetTable.replace(slot.timeMs,nextMessage.getStoreSize()+nowReadOffset);
        // System.out.printf("now message size:%d%n",nextMessage.getStoreSize());
        return nextMessage;
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

    public long deleteTimeMs(long timeMs) {
        return (timeMs / precisionMs - deleteDelaySlot) * precisionMs;
    }

    public long preTickTimeMs(long timeMs) {
        return (timeMs / precisionMs + tickBeforeSlot) * precisionMs;
    }

    public void deleteExpiredItems() {
        try {
            if (slotMaxOffsetTable.size() > 0 && slotReadOffsetTable.size() > 0) {
                for (Long item : slotMaxOffsetTable.keySet()) {
                    if (item == null) {
                        break;
                    }
                    Long readOffset = slotReadOffsetTable.get(item);
                    Long maxOffset = slotMaxOffsetTable.get(item);
                    if(readOffset == null || maxOffset == null){
                        continue;
                    }
                    if (readOffset >= maxOffset && item < deleteTimeMs(System.currentTimeMillis())) {
                        System.out.printf("Now item deleted:%d%n", item);
                        slotMaxOffsetTable.remove(item);
                        slotReadOffsetTable.remove(item);
                        // slotTable.get(item).clear();
                        slotTable.remove(item);
                    }
                }
                if(this.nextWheel!=null) {
                    this.nextWheel.deleteExpiredItems();
                }
            }
        } catch (Exception e){
            // System.out.printf("error:"+e);
        }
    }

    public void printAllSlot() {
        if(this.nextWheel!=null){
            this.nextWheel.printAllSlot();
        }
        Date nowDate = new Date();
        System.out.printf("-----------------[Now time"+nowDate+"]----------------\n");
        System.out.printf("---------[Wheel Precision %d]--------\n",precisionMs);
        System.out.printf("Now slot num:%d\n",slotTable.size());
        System.out.printf("Now max slot offset num:%d\n",slotMaxOffsetTable.size());
        System.out.printf("Now read slot offset num:%d\n",slotReadOffsetTable.size());
        List<Long> keyList = Collections.list(slotMaxOffsetTable.keys());
        Collections.sort(keyList);
        for(int i=0; i<slotMaxOffsetTable.size();i++){
            long timeStamp = keyList.get(i);
            Date date = new Date();
            date.setTime(timeStamp);
            Long maxOffset = slotMaxOffsetTable.get(timeStamp);
            Long readOffset = slotReadOffsetTable.get(timeStamp);
            System.out.printf("[Slot "+date+"] Max offset:"+maxOffset/1024+" KB, read: "+(readOffset==null? 0 :readOffset/1024)+"\n");
        }
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
        System.out.printf("Timer Wheel recover need to be rewrite.");
        return 0L;
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
