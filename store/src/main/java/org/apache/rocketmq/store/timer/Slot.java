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

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class Slot {
    public static final short SIZE = 16;
    public SlotLog slotLog;
    MessageStoreConfig storeConfig = new MessageStoreConfig();
    public final long timeMs;
    public final long firstPos;
    public final long lastPos;
    public final int num;
    public final int magic; //no use now, just keep it

    public Slot(long timeMs, long firstPos, long lastPos) {
        this.timeMs = timeMs;
        this.slotLog = new SlotLog(timeMs, storeConfig, null);
        this.firstPos = firstPos;
        this.lastPos = lastPos;
        this.num = 0;
        this.magic = 0;
    }

    //TODO: only keep this one.
    public Slot(long timeMs, int num, int magic) {
        this.timeMs = timeMs;
        this.slotLog = new SlotLog(timeMs, storeConfig, null);
        this.slotLog.load();
        this.num = num;
        this.magic = magic;

        this.firstPos = 0;
        this.lastPos = 0;
    }

    public Slot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        this.timeMs = timeMs;
        this.firstPos = firstPos;
        this.lastPos = lastPos;
        this.num = num;
        this.magic = magic;
    }

    public void putMessage(final MessageExt msg) throws Exception {
        this.slotLog.putMessage(msg);
    }
    public MessageExt getNextMessage(){
        return this.slotLog.getNextMessage();
    }

    public void setMaxFlushedWhere(long maxFlushedWhere){
        this.slotLog.mappedFileQueue.setFlushedWhere(maxFlushedWhere);
    }
}
