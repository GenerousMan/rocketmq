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
package org.apache.rocketmq.store.rocksdb;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.store.MessageStore;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/*
kDataBlockBinaryAndHash
https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
 */
public class RocksDBOptionsFactory {

    public static ColumnFamilyOptions createCQCFOptions(final MessageStore messageStore) {
        BlockBasedTableConfig tconfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kBinarySearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash).
                setDataBlockHashTableUtilRatio(0.75).
                setBlockSize(32 * SizeUnit.KB).
                setMetadataBlockSize(4 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(1024 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        CompactionOptionsUniversal compactionOption = new CompactionOptionsUniversal();
        compactionOption.setSizeRatio(100).
                setMaxSizeAmplificationPercent(25).
                setAllowTrivialMove(true).
                setMinMergeWidth(2).
                setMaxMergeWidth(Integer.MAX_VALUE).
                setStopStyle(CompactionStopStyle.CompactionStopStyleTotalSize).
                setCompressionSizePercent(-1);
        return options.setMaxWriteBufferNumber(4).
                setWriteBufferSize(128 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(tconfig).
                setMemTableConfig(new SkipListMemTableConfig()).
                setCompressionType(CompressionType.LZ4_COMPRESSION).
                setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION).
                setNumLevels(7).
                setCompactionStyle(CompactionStyle.UNIVERSAL).
                setCompactionOptionsUniversal(compactionOption).
                setMaxCompactionBytes(100 * SizeUnit.GB).
                setSoftPendingCompactionBytesLimit(100 * SizeUnit.GB).
                setHardPendingCompactionBytesLimit(256 * SizeUnit.GB).
                setLevel0FileNumCompactionTrigger(2).
                setLevel0SlowdownWritesTrigger(8).
                setLevel0StopWritesTrigger(10).
                setTargetFileSizeBase(256 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setReportBgIoStats(true).
                setOptimizeFiltersForHits(true);
    }

    public static ColumnFamilyOptions createOffsetCFOptions() {
        BlockBasedTableConfig tconfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kBinarySearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch).
                setBlockSize(32 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(128 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        return options.setMaxWriteBufferNumber(4).
                setWriteBufferSize(64 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(tconfig).
                setMemTableConfig(new SkipListMemTableConfig()).
                setCompressionType(CompressionType.NO_COMPRESSION).
                setNumLevels(7).
                setCompactionStyle(CompactionStyle.LEVEL).
                setLevel0FileNumCompactionTrigger(2).
                setLevel0SlowdownWritesTrigger(8).
                setLevel0StopWritesTrigger(10).
                setTargetFileSizeBase(64 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMaxBytesForLevelBase(256 * SizeUnit.MB).
                setMaxBytesForLevelMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setInplaceUpdateSupport(true);
    }

    public static ColumnFamilyOptions createTimerCFOptions(final TimerMessageStore timerMessageStore) {
        BlockBasedTableConfig tconfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kBinarySearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash).
                setDataBlockHashTableUtilRatio(0.75).
                setBlockSize(32 * SizeUnit.KB).
                setMetadataBlockSize(4 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(128 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        CompactionOptionsUniversal compactionOption = new CompactionOptionsUniversal();
        compactionOption.setSizeRatio(100).
                setMaxSizeAmplificationPercent(50).
                setAllowTrivialMove(true).
                setMinMergeWidth(2).
                setMaxMergeWidth(Integer.MAX_VALUE).
                setStopStyle(CompactionStopStyle.CompactionStopStyleTotalSize).
                setCompressionSizePercent(-1);
        return options.setMemTableConfig(new SkipListMemTableConfig()).
                setMaxWriteBufferNumber(4).
                setWriteBufferSize(128 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(tconfig).
                setCompactionStyle(CompactionStyle.UNIVERSAL).
                setCompactionOptionsUniversal(compactionOption).
                setNumLevels(7).
                setCompressionType(CompressionType.LZ4_COMPRESSION).
                setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION).
                setMaxCompactionBytes(100 * SizeUnit.GB).
                setSoftPendingCompactionBytesLimit(100 * SizeUnit.GB).
                setHardPendingCompactionBytesLimit(150 * SizeUnit.GB).
                setLevel0FileNumCompactionTrigger(4).
                setLevel0SlowdownWritesTrigger(18).
                setLevel0StopWritesTrigger(20).
                setTargetFileSizeBase(256 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setReportBgIoStats(true).
                setOptimizeFiltersForHits(true);
    }

    public static ColumnFamilyOptions createTimerCFLevelOptions(final TimerMessageStore timerMessageStore, final int prefixLen) {
        BlockBasedTableConfig tconfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kHashSearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash).
                setDataBlockHashTableUtilRatio(0.75).
                setBlockSize(32 * SizeUnit.KB).
                setMetadataBlockSize(4 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(128 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        return options.setMemTableConfig(new HashSkipListMemTableConfig()).
                useFixedLengthPrefixExtractor(prefixLen).
                setMaxWriteBufferNumber(4).
                setWriteBufferSize(128 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(tconfig).
                setCompressionType(CompressionType.LZ4_COMPRESSION).
                setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION).
                setNumLevels(7).
                setCompactionStyle(CompactionStyle.LEVEL).
                setMaxCompactionBytes(100 * SizeUnit.GB).
                setSoftPendingCompactionBytesLimit(100 * SizeUnit.GB).
                setHardPendingCompactionBytesLimit(150 * SizeUnit.GB).
                setLevel0FileNumCompactionTrigger(6).
                setLevel0SlowdownWritesTrigger(12).
                setLevel0StopWritesTrigger(18).
                setTargetFileSizeBase(128 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMaxBytesForLevelBase(256 * SizeUnit.MB).
                setMaxBytesForLevelMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setReportBgIoStats(true);
    }

    public static ColumnFamilyOptions createConfigOptions() {
        BlockBasedTableConfig tconfig = new BlockBasedTableConfig().
                setFormatVersion(5).
                setIndexType(IndexType.kBinarySearch).
                setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch).
                setBlockSize(32 * SizeUnit.KB).
                setFilterPolicy(new BloomFilter(16, false)).
                setCacheIndexAndFilterBlocks(false).
                setCacheIndexAndFilterBlocksWithHighPriority(true).
                setPinL0FilterAndIndexBlocksInCache(false).
                setPinTopLevelIndexAndFilter(true).
                setBlockCache(new LRUCache(4 * SizeUnit.MB, 8, false)).
                setWholeKeyFiltering(true);

        ColumnFamilyOptions options = new ColumnFamilyOptions();
        return options.setMaxWriteBufferNumber(2).
                setWriteBufferSize(8 * SizeUnit.MB).
                setMinWriteBufferNumberToMerge(1).
                setTableFormatConfig(tconfig).
                setMemTableConfig(new SkipListMemTableConfig()).
                setCompressionType(CompressionType.NO_COMPRESSION).
                setNumLevels(7).
                setCompactionStyle(CompactionStyle.LEVEL).
                setLevel0FileNumCompactionTrigger(4).
                setLevel0SlowdownWritesTrigger(8).
                setLevel0StopWritesTrigger(12).
                setTargetFileSizeBase(64 * SizeUnit.MB).
                setTargetFileSizeMultiplier(2).
                setMaxBytesForLevelBase(256 * SizeUnit.MB).
                setMaxBytesForLevelMultiplier(2).
                setMergeOperator(new StringAppendOperator()).
                setInplaceUpdateSupport(true);
    }

    /**
     * Create a rocksdb db options, the user must take care to close it after closing db.
     * @return
     */
    public static DBOptions createDBOptions() {
        //Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        // and http://gitlab.alibaba-inc.com/aloha/aloha/blob/branch_2_5_0/jstorm-core/src/main/java/com/alibaba/jstorm/cache/rocksdb/RocksDbOptionsFactory.java
        DBOptions options = new DBOptions();
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return options.
                setDbLogDir(getDBLogDir()).
                setInfoLogLevel(InfoLogLevel.INFO_LEVEL).
                setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery).
                setManualWalFlush(true).
                setMaxTotalWalSize(0).
                setWalSizeLimitMB(0).
                setWalTtlSeconds(0).
                setCreateIfMissing(true).
                setCreateMissingColumnFamilies(true).
                setMaxOpenFiles(-1).
                setMaxLogFileSize(1 * SizeUnit.GB).
                setKeepLogFileNum(5).
                setMaxManifestFileSize(1 * SizeUnit.GB).
                setAllowConcurrentMemtableWrite(false).
                setStatistics(statistics).
                setAtomicFlush(true).
                setMaxBackgroundJobs(32).
                setMaxSubcompactions(8).
                setParanoidChecks(true).
                setDelayedWriteRate(16 * SizeUnit.MB).
                setRateLimiter(new RateLimiter(100 * SizeUnit.MB)).
                setUseDirectIoForFlushAndCompaction(false).
                setUseDirectReads(false);
    }

    public static DBOptions createConfigDBOptions() {
        //Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        // and http://gitlab.alibaba-inc.com/aloha/aloha/blob/branch_2_5_0/jstorm-core/src/main/java/com/alibaba/jstorm/cache/rocksdb/RocksDbOptionsFactory.java
        DBOptions options = new DBOptions();
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return options.
                setDbLogDir(getDBLogDir()).
                setInfoLogLevel(InfoLogLevel.INFO_LEVEL).
                setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords).
                setManualWalFlush(true).
                setMaxTotalWalSize(500 * SizeUnit.MB).
                setWalSizeLimitMB(0).
                setWalTtlSeconds(0).
                setCreateIfMissing(true).
                setCreateMissingColumnFamilies(true).
                setMaxOpenFiles(-1).
                setMaxLogFileSize(1 * SizeUnit.GB).
                setKeepLogFileNum(5).
                setMaxManifestFileSize(1 * SizeUnit.GB).
                setAllowConcurrentMemtableWrite(false).
                setStatistics(statistics).
                setStatsDumpPeriodSec(600).
                setAtomicFlush(true).
                setMaxBackgroundJobs(32).
                setMaxSubcompactions(4).
                setParanoidChecks(true).
                setDelayedWriteRate(16 * SizeUnit.MB).
                setRateLimiter(new RateLimiter(100 * SizeUnit.MB)).
                setUseDirectIoForFlushAndCompaction(true).
                setUseDirectReads(true);
    }

    public static DBOptions createTimerDBOptions() {
        DBOptions options = new DBOptions();
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        return options.
                setDbLogDir(getDBLogDir()).
                setInfoLogLevel(InfoLogLevel.INFO_LEVEL).
                setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords).
                setManualWalFlush(true).
                setMaxTotalWalSize(500 * SizeUnit.MB).
                setWalSizeLimitMB(0).
                setWalTtlSeconds(0).
                setCreateIfMissing(true).
                setCreateMissingColumnFamilies(true).
                setMaxOpenFiles(-1).
                setMaxLogFileSize(1 * SizeUnit.GB).
                setKeepLogFileNum(5).
                setMaxManifestFileSize(1 * SizeUnit.GB).
                setAllowConcurrentMemtableWrite(true).
                setEnableWriteThreadAdaptiveYield(true).
                setStatistics(statistics).
                setAtomicFlush(false).
                setMaxBackgroundJobs(32).
                setMaxSubcompactions(8).
                setParanoidChecks(true).
                setDelayedWriteRate(16 * SizeUnit.MB).
                setRateLimiter(new RateLimiter(100 * SizeUnit.MB)).
                setUseDirectIoForFlushAndCompaction(true).
                setUseDirectReads(true);
    }

//    /**
//     * set rocksdb env for pangu2, must be inited before open rocksdb
//     * @param storeConfig
//     * @param env
//     * @param options
//     * @param storeType
//     */
//    public static void initRocksDBConfig(MessageStoreConfig storeConfig, Env env, DBOptionsInterface options,
//                                         StoreType storeType) {
//        if (storeConfig != null) {
//            rocksDBBlockCacheMB = storeConfig.getRocksDBBlockCacheMB();
//            if (storeType != StoreType.DFS) {
//                return;
//            }
//            if (DfsUtil.isPanguMode()) {
//                env = Env.getPanguEnv(DfsUtil.getDfsDefaultFS(), true);
//                options.setEnv(env);
//                return;
//            }
//            if (DfsUtil.isAliDfsMode()) {
//                env = Env.getPdfsEnv(DfsUtil.getDfsDefaultFS());
//                options.setEnv(env);
//                return;
//            }
//        }
//    }

    private static String getDBLogDir() {
        String rootPath = System.getProperty("user.home");
        if (StringUtils.isEmpty(rootPath)) {
            return "";
        }
        return rootPath + "/logs/rocketmqlogs/";
    }
}
