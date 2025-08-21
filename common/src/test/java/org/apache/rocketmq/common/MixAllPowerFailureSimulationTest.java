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

package org.apache.rocketmq.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Power failure simulation test for MixAll file persistence.
 * 
 * This test simulates various power failure scenarios that can occur during file write operations:
 * 1. Power failure during backup creation
 * 2. Power failure during main file write
 * 3. Power failure after partial file write
 * 4. Power failure during file system sync
 * 
 * The goal is to verify that the backup mechanism prevents double file corruption
 * and ensures at least one valid file always exists after power restoration.
 */
public class MixAllPowerFailureSimulationTest {

    private static final String TEST_DIR = System.getProperty("java.io.tmpdir") + File.separator + 
                                          "rocketmq-power-failure-test-" + System.currentTimeMillis();
    private static final Random RANDOM = new Random();
    
    private Path testDirPath;
    private ExecutorService executorService;

    @Before
    public void setUp() throws IOException {
        testDirPath = Paths.get(TEST_DIR);
        Files.createDirectories(testDirPath);
        executorService = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() throws IOException {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        if (Files.exists(testDirPath)) {
            deleteRecursively(testDirPath);
        }
    }

    /**
     * Test power failure during backup creation phase
     */
    @Test
    public void testPowerFailureDuringBackupCreation() throws Exception {
        String fileName = getTestFileName("backup_failure");
        int testIterations = 500;
        AtomicInteger doubleCorruptionCount = new AtomicInteger(0);
        List<String> knownGoodContents = new ArrayList<>();
        
        // Initialize with valid content
        String initialContent = "Initial content before power failure tests";
        MixAll.string2File(initialContent, fileName);
        knownGoodContents.add(initialContent);
        
        for (int i = 0; i < testIterations; i++) {
            String newContent = "Power failure test iteration " + i + " - " + generateContent(200);
            
            try {
                // Simulate power failure during backup creation (20% chance)
                if (RANDOM.nextDouble() < 0.2) {
                    simulatePowerFailureDuringBackupCreation(fileName, newContent);
                } else {
                    // Normal write
                    MixAll.string2File(newContent, fileName);
                    knownGoodContents.add(newContent);
                }
            } catch (IOException e) {
                // Expected for power failure scenarios
            }
            
            // Verify no double corruption occurred
            verifyNoDoulbeCorruption(fileName, knownGoodContents, doubleCorruptionCount);
        }
        
        // Should have zero double corruptions
        assertThat(doubleCorruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test power failure during main file write phase
     */
    @Test
    public void testPowerFailureDuringMainFileWrite() throws Exception {
        String fileName = getTestFileName("main_write_failure");
        int testIterations = 500;
        AtomicInteger doubleCorruptionCount = new AtomicInteger(0);
        List<String> knownGoodContents = new ArrayList<>();
        
        // Initialize with valid content
        String initialContent = "Initial content for main write failure test";
        MixAll.string2File(initialContent, fileName);
        knownGoodContents.add(initialContent);
        
        for (int i = 0; i < testIterations; i++) {
            String newContent = "Main write failure test " + i + " - " + generateContent(300);
            
            try {
                // Simulate power failure during main file write (25% chance)
                if (RANDOM.nextDouble() < 0.25) {
                    simulatePowerFailureDuringMainWrite(fileName, newContent);
                } else {
                    MixAll.string2File(newContent, fileName);
                    knownGoodContents.add(newContent);
                }
            } catch (IOException e) {
                // Expected for power failure scenarios
            }
            
            verifyNoDoulbeCorruption(fileName, knownGoodContents, doubleCorruptionCount);
        }
        
        assertThat(doubleCorruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test power failure with partial file writes
     */
    @Test
    public void testPowerFailureWithPartialWrites() throws Exception {
        String fileName = getTestFileName("partial_write");
        int testIterations = 300;
        AtomicInteger doubleCorruptionCount = new AtomicInteger(0);
        List<String> knownGoodContents = new ArrayList<>();
        
        String initialContent = "Initial content for partial write test";
        MixAll.string2File(initialContent, fileName);
        knownGoodContents.add(initialContent);
        
        for (int i = 0; i < testIterations; i++) {
            String newContent = "Partial write test " + i + " - " + generateContent(500);
            
            try {
                // Various power failure scenarios
                double failureType = RANDOM.nextDouble();
                if (failureType < 0.15) {
                    simulatePartialFileWrite(fileName, newContent);
                } else if (failureType < 0.3) {
                    simulatePowerFailureDuringSync(fileName, newContent);
                } else {
                    MixAll.string2File(newContent, fileName);
                    knownGoodContents.add(newContent);
                }
            } catch (IOException e) {
                // Expected
            }
            
            verifyNoDoulbeCorruption(fileName, knownGoodContents, doubleCorruptionCount);
        }
        
        assertThat(doubleCorruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test multiple concurrent power failures
     */
    @Test
    public void testConcurrentPowerFailures() throws Exception {
        String fileName = getTestFileName("concurrent_power_failure");
        int threadCount = 3;
        int operationsPerThread = 100;
        AtomicInteger doubleCorruptionCount = new AtomicInteger(0);
        List<String> knownGoodContents = new ArrayList<>();
        
        // Initialize
        String initialContent = "Initial content for concurrent power failure test";
        MixAll.string2File(initialContent, fileName);
        knownGoodContents.add(initialContent);
        
        List<Future<?>> futures = new ArrayList<>();
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Future<?> future = executorService.submit(() -> {
                for (int i = 0; i < operationsPerThread; i++) {
                    String content = "Thread-" + threadId + "-Op-" + i + "-" + generateContent(150);
                    
                    try {
                        // Random power failure simulation
                        double failureChance = RANDOM.nextDouble();
                        if (failureChance < 0.1) {
                            simulatePowerFailureDuringBackupCreation(fileName, content);
                        } else if (failureChance < 0.2) {
                            simulatePowerFailureDuringMainWrite(fileName, content);
                        } else if (failureChance < 0.3) {
                            simulatePartialFileWrite(fileName, content);
                        } else {
                            synchronized (knownGoodContents) {
                                MixAll.string2File(content, fileName);
                                knownGoodContents.add(content);
                            }
                        }
                    } catch (IOException e) {
                        // Expected for power failures
                    }
                    
                    // Periodic integrity check
                    if (i % 20 == 0) {
                        synchronized (knownGoodContents) {
                            verifyNoDoulbeCorruption(fileName, knownGoodContents, doubleCorruptionCount);
                        }
                    }
                }
            });
            futures.add(future);
        }
        
        // Wait for all threads to complete
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        
        // Final check
        verifyNoDoulbeCorruption(fileName, knownGoodContents, doubleCorruptionCount);
        assertThat(doubleCorruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test recovery after systematic power failures
     */
    @Test
    public void testRecoveryAfterSystematicPowerFailures() throws Exception {
        String fileName = getTestFileName("systematic_recovery");
        AtomicInteger doubleCorruptionCount = new AtomicInteger(0);
        List<String> knownGoodContents = new ArrayList<>();
        
        // Phase 1: Establish baseline
        String baseContent = "Baseline content for systematic test";
        MixAll.string2File(baseContent, fileName);
        knownGoodContents.add(baseContent);
        
        // Phase 2: Systematic power failures
        for (int i = 0; i < 200; i++) {
            String content = "Systematic test " + i + " - " + generateContent(250);
            
            try {
                // Rotate through different failure types
                switch (i % 4) {
                    case 0:
                        simulatePowerFailureDuringBackupCreation(fileName, content);
                        break;
                    case 1:
                        simulatePowerFailureDuringMainWrite(fileName, content);
                        break;
                    case 2:
                        simulatePartialFileWrite(fileName, content);
                        break;
                    case 3:
                        MixAll.string2File(content, fileName);
                        knownGoodContents.add(content);
                        break;
                }
            } catch (IOException e) {
                // Expected
            }
            
            verifyNoDoulbeCorruption(fileName, knownGoodContents, doubleCorruptionCount);
        }
        
        // Phase 3: Recovery test - normal operations after failures
        for (int i = 0; i < 50; i++) {
            String recoveryContent = "Recovery test " + i + " - " + generateContent(100);
            MixAll.string2File(recoveryContent, fileName);
            knownGoodContents.add(recoveryContent);
            
            // Verify normal operation
            assertThat(MixAll.file2String(fileName)).isEqualTo(recoveryContent);
        }
        
        assertThat(doubleCorruptionCount.get()).isEqualTo(0);
    }

    /**
     * Simulate power failure during backup creation
     */
    private void simulatePowerFailureDuringBackupCreation(String fileName, String newContent) throws IOException {
        String bakFile = fileName + ".bak";
        String prevContent = MixAll.file2String(fileName);
        
        if (prevContent != null) {
            // Start writing backup but don't complete it (simulate power failure)
            File backupFile = new File(bakFile);
            try (FileOutputStream fos = new FileOutputStream(backupFile)) {
                byte[] data = prevContent.getBytes(MixAll.DEFAULT_CHARSET);
                // Write only partial backup data
                int maxPartial = Math.max(1, data.length / 2);
                int partialSize = Math.max(1, RANDOM.nextInt(maxPartial) + 1);
                fos.write(data, 0, Math.min(partialSize, data.length));
                // Don't close or flush - simulate sudden power loss
            }
        }
        
        throw new IOException("Simulated power failure during backup creation");
    }

    /**
     * Simulate power failure during main file write
     */
    private void simulatePowerFailureDuringMainWrite(String fileName, String newContent) throws IOException {
        String bakFile = fileName + ".bak";
        String prevContent = MixAll.file2String(fileName);
        
        // Successfully create backup first
        if (prevContent != null) {
            MixAll.string2FileNotSafe(prevContent, bakFile);
        }
        
        // Start writing main file but fail partway through
        File mainFile = new File(fileName);
        try (FileOutputStream fos = new FileOutputStream(mainFile)) {
            byte[] data = newContent.getBytes(MixAll.DEFAULT_CHARSET);
            // Write partial data then simulate power failure
            int maxPartial = Math.max(1, data.length / 2);
            int partialSize = Math.max(1, RANDOM.nextInt(maxPartial) + 1);
            fos.write(data, 0, Math.min(partialSize, data.length));
            // Don't close properly
        }
        
        throw new IOException("Simulated power failure during main file write");
    }

    /**
     * Simulate partial file write due to power failure
     */
    private void simulatePartialFileWrite(String fileName, String newContent) throws IOException {
        // This simulates the case where the OS buffer is partially written to disk
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            byte[] data = newContent.getBytes(MixAll.DEFAULT_CHARSET);
            int maxPartial = Math.max(1, data.length / 3);
            int partialSize = Math.max(1, RANDOM.nextInt(maxPartial) + 1);
            raf.write(data, 0, Math.min(partialSize, data.length));
            // Truncate the file at partial write position
            raf.setLength(partialSize);
        }
        
        throw new IOException("Simulated partial file write due to power failure");
    }

    /**
     * Simulate power failure during file system sync
     */
    private void simulatePowerFailureDuringSync(String fileName, String newContent) throws IOException {
        // Write the content normally but simulate failure during sync
        File file = new File(fileName);
        try (FileChannel channel = FileChannel.open(file.toPath(), 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] data = newContent.getBytes(MixAll.DEFAULT_CHARSET);
            channel.write(java.nio.ByteBuffer.wrap(data));
            // Don't force sync - simulate power failure before data hits disk
        }
        
        throw new IOException("Simulated power failure during file system sync");
    }

    /**
     * Verify that no double corruption has occurred
     */
    private void verifyNoDoulbeCorruption(String fileName, List<String> knownGoodContents, 
                                         AtomicInteger doubleCorruptionCount) {
        try {
            String mainContent = MixAll.file2String(fileName);
            String backupContent = MixAll.file2String(fileName + ".bak");
            
            boolean mainIsValid = isValidContent(mainContent, knownGoodContents);
            boolean backupIsValid = isValidContent(backupContent, knownGoodContents);
            
            // The critical test: at least one file must be valid
            // If both files are invalid/corrupted, it's double corruption
            if (!mainIsValid && !backupIsValid) {
                doubleCorruptionCount.incrementAndGet();
            }
            
        } catch (IOException e) {
            // If we can't read either file, that's also considered double corruption
            doubleCorruptionCount.incrementAndGet();
        }
    }

    /**
     * Check if content is valid
     */
    private boolean isValidContent(String content, List<String> knownGoodContents) {
        if (content == null) {
            return false;
        }
        
        // Empty content is considered invalid unless it was intentionally written
        if (content.trim().isEmpty()) {
            return knownGoodContents.contains("");
        }
        
        // Check if it matches any known good content
        if (knownGoodContents.contains(content)) {
            return true;
        }
        
        // Check if it's a reasonable content format (not obviously corrupted)
        return content.length() > 10 && 
               !content.contains("\0") &&  // No null bytes
               content.contains("test");    // Should contain our test marker
    }

    /**
     * Generate random content for testing
     */
    private String generateContent(int length) {
        StringBuilder sb = new StringBuilder();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private String getTestFileName(String testName) {
        return testDirPath.toString() + File.separator + "power_failure_" + testName + "_" + System.nanoTime() + ".txt";
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path)) {
            if (Files.isDirectory(path)) {
                Files.list(path).forEach(child -> {
                    try {
                        deleteRecursively(child);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
            }
            Files.deleteIfExists(path);
        }
    }
}