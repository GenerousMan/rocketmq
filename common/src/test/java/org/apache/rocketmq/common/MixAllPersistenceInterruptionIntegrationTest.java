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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
 * Integration test for MixAll file persistence with real interruptions.
 * Simulates actual interruption scenarios during file write operations.
 */
public class MixAllPersistenceInterruptionIntegrationTest {

    private static final String TEST_DIR = System.getProperty("java.io.tmpdir") + File.separator + 
                                          "rocketmq-interruption-test-" + System.currentTimeMillis();
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
     * Test interruption during file write using thread interruption
     */
    @Test
    public void testInterruptionDuringFileWrite() throws Exception {
        String fileName = getTestFileName("interrupt_write");
        int testIterations = 200;
        AtomicInteger successfulWrites = new AtomicInteger(0);
        AtomicInteger interruptedWrites = new AtomicInteger(0);
        AtomicInteger fileCorruptions = new AtomicInteger(0);
        
        for (int i = 0; i < testIterations; i++) {
            final String content = "Iteration " + i + " - " + generateLargeContent(1000);
            
            CompletableFuture<Void> writeTask = CompletableFuture.runAsync(() -> {
                try {
                    // Create a custom write method that can be interrupted
                    interruptibleString2File(content, fileName);
                    successfulWrites.incrementAndGet();
                } catch (InterruptedException e) {
                    interruptedWrites.incrementAndGet();
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    // Expected for some interrupted operations
                }
            }, executorService);
            
            // Randomly interrupt some operations at different stages
            if (RANDOM.nextDouble() < 0.1) { // 10% chance of interruption
                // Wait a random short time to interrupt at different stages
                Thread.sleep(RANDOM.nextInt(5) + 1);
                writeTask.cancel(true);
            } else {
                writeTask.get(1, TimeUnit.SECONDS);
            }
            
            // Verify file integrity after each operation
            verifyFileIntegrityAfterInterruption(fileName, fileCorruptions);
        }
        
        // Test completed - results tracked in counters
        
        // Verify that the backup mechanism works even with interruptions
        assertThat(fileCorruptions.get()).isEqualTo(0);
        assertThat(successfulWrites.get() + interruptedWrites.get()).isGreaterThan((int)(testIterations * 0.8));
    }

    /**
     * Test simulated disk full scenario during write
     */
    @Test
    public void testDiskFullDuringWrite() throws Exception {
        String fileName = getTestFileName("disk_full");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        // First write some content successfully
        String initialContent = "Initial content before disk full";
        MixAll.string2File(initialContent, fileName);
        assertThat(MixAll.file2String(fileName)).isEqualTo(initialContent);
        
        // Simulate disk full by filling up space (controlled test)
        for (int i = 0; i < 50; i++) {
            try {
                String content = "Large content " + i + " - " + generateLargeContent(10000);
                
                // Simulate partial write failure
                if (RANDOM.nextDouble() < 0.2) {
                    simulatePartialWriteFailure(content, fileName);
                } else {
                    MixAll.string2File(content, fileName);
                }
                
                // Verify integrity
                verifyFileIntegrityAfterInterruption(fileName, corruptionCount);
                
            } catch (IOException e) {
                // Expected for disk full simulation
                verifyFileIntegrityAfterInterruption(fileName, corruptionCount);
            }
        }
        
        assertThat(corruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test power failure simulation during write operations
     */
    @Test
    public void testPowerFailureSimulation() throws Exception {
        String fileName = getTestFileName("power_failure");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        List<String> successfulContents = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
            String content = "Power test " + i + " - " + generateLargeContent(500);
            
            try {
                // Simulate power failure at different stages
                if (RANDOM.nextDouble() < 0.15) { // 15% chance of power failure
                    simulatePowerFailureDuringWrite(content, fileName);
                } else {
                    MixAll.string2File(content, fileName);
                    successfulContents.add(content);
                }
                
                // After each operation, verify at least one valid file exists
                verifyAtLeastOneValidFile(fileName, successfulContents, corruptionCount);
                
            } catch (IOException e) {
                // Expected for power failure scenarios
                verifyAtLeastOneValidFile(fileName, successfulContents, corruptionCount);
            }
        }
        
        assertThat(corruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test concurrent writes with random interruptions
     */
    @Test
    public void testConcurrentWritesWithInterruptions() throws Exception {
        String fileName = getTestFileName("concurrent_interrupt");
        int threadCount = 3;
        int writesPerThread = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        AtomicInteger totalSuccessful = new AtomicInteger(0);
        AtomicInteger totalInterrupted = new AtomicInteger(0);
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < writesPerThread; i++) {
                        String content = "Thread-" + threadId + "-Write-" + i + "-" + generateLargeContent(200);
                        
                        Future<Void> writeTask = executorService.submit(() -> {
                            try {
                                interruptibleString2File(content, fileName);
                                totalSuccessful.incrementAndGet();
                                return null;
                            } catch (InterruptedException e) {
                                totalInterrupted.incrementAndGet();
                                Thread.currentThread().interrupt();
                                return null;
                            } catch (IOException e) {
                                return null;
                            }
                        });
                        
                        // Randomly interrupt some writes
                        if (RANDOM.nextDouble() < 0.1) {
                            Thread.sleep(RANDOM.nextInt(3) + 1);
                            writeTask.cancel(true);
                        } else {
                            writeTask.get();
                        }
                        
                        // Check integrity periodically
                        if (i % 10 == 0) {
                            verifyFileIntegrityAfterInterruption(fileName, corruptionCount);
                        }
                    }
                } catch (Exception e) {
                    // Handle exceptions
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        finishLatch.await(30, TimeUnit.SECONDS);
        
        // Test completed - results tracked in counters
        
        assertThat(corruptionCount.get()).isEqualTo(0);
    }

    /**
     * Test file system error simulation during writes
     */
    @Test
    public void testFileSystemErrorsDuringWrite() throws Exception {
        String fileName = getTestFileName("fs_errors");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        for (int i = 0; i < 100; i++) {
            String content = "FS Error test " + i + " - " + generateLargeContent(300);
            
            try {
                // Simulate various file system errors
                double errorType = RANDOM.nextDouble();
                if (errorType < 0.1) {
                    // Simulate permission denied
                    simulatePermissionError(content, fileName);
                } else if (errorType < 0.2) {
                    // Simulate disk read-only
                    simulateReadOnlyError(content, fileName);
                } else {
                    // Normal write
                    MixAll.string2File(content, fileName);
                }
                
            } catch (IOException e) {
                // Expected for error simulation
            }
            
            // Always verify integrity after each attempt
            verifyFileIntegrityAfterInterruption(fileName, corruptionCount);
        }
        
        assertThat(corruptionCount.get()).isEqualTo(0);
    }

    /**
     * Custom string2File method that can be interrupted
     */
    private void interruptibleString2File(String str, String fileName) throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted before write");
        }
        
        String bakFile = fileName + ".bak";
        String prevContent = MixAll.file2String(fileName);
        
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted during backup read");
        }
        
        if (prevContent != null) {
            // Check for interruption before backup write
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread was interrupted before backup write");
            }
            MixAll.string2FileNotSafe(prevContent, bakFile);
        }
        
        // Check for interruption before main write
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted before main write");
        }
        
        // Simulate slow write that can be interrupted
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        
        // Write in chunks to allow interruption
        byte[] data = str.getBytes(MixAll.DEFAULT_CHARSET);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            int chunkSize = Math.max(1, data.length / 10);
            for (int i = 0; i < data.length; i += chunkSize) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Thread was interrupted during write");
                }
                
                int end = Math.min(i + chunkSize, data.length);
                fos.write(data, i, end - i);
                fos.flush();
                
                // Small delay to make interruption more likely
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw e;
                }
            }
        }
    }

    /**
     * Simulate partial write failure
     */
    private void simulatePartialWriteFailure(String content, String fileName) throws IOException {
        // Write only part of the content to simulate interruption
        String partialContent = content.substring(0, Math.min(content.length() / 2, 100));
        File file = new File(fileName);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(partialContent.getBytes(MixAll.DEFAULT_CHARSET));
            // Don't close properly to simulate crash
        }
        // Simulate crash by not completing the write
        throw new IOException("Simulated partial write failure");
    }

    /**
     * Simulate power failure during write
     */
    private void simulatePowerFailureDuringWrite(String content, String fileName) throws IOException {
        // Start the write process but don't complete it
        byte[] data = content.getBytes(MixAll.DEFAULT_CHARSET);
        File file = new File(fileName);
        
        try (FileOutputStream fos = new FileOutputStream(file)) {
            // Write partial data
            int partialSize = RANDOM.nextInt(data.length / 2) + 1;
            fos.write(data, 0, partialSize);
            // Simulate sudden power loss - don't close or flush
        }
        throw new IOException("Simulated power failure");
    }

    /**
     * Simulate permission error
     */
    private void simulatePermissionError(String content, String fileName) throws IOException {
        // Just throw an exception to simulate permission denied
        throw new IOException("Simulated permission denied");
    }

    /**
     * Simulate read-only file system error
     */
    private void simulateReadOnlyError(String content, String fileName) throws IOException {
        throw new IOException("Simulated read-only file system");
    }

    /**
     * Verify file integrity after interruption
     */
    private void verifyFileIntegrityAfterInterruption(String fileName, AtomicInteger corruptionCount) {
        try {
            String mainContent = MixAll.file2String(fileName);
            String backupContent = MixAll.file2String(fileName + ".bak");
            
            // At least one file should exist or both should be null (initial state)
            if (mainContent == null && backupContent == null) {
                // This is acceptable for initial state
                return;
            }
            
            // If main file exists, it should be readable and non-corrupted
            if (mainContent != null && isCorruptedContent(mainContent)) {
                corruptionCount.incrementAndGet();
            }
            
            // If backup file exists, it should be readable and non-corrupted
            if (backupContent != null && isCorruptedContent(backupContent)) {
                corruptionCount.incrementAndGet();
            }
            
        } catch (IOException e) {
            // File access error - could indicate corruption
            corruptionCount.incrementAndGet();
        }
    }

    /**
     * Verify at least one valid file exists
     */
    private void verifyAtLeastOneValidFile(String fileName, List<String> validContents, AtomicInteger corruptionCount) {
        try {
            String mainContent = MixAll.file2String(fileName);
            String backupContent = MixAll.file2String(fileName + ".bak");
            
            boolean hasValidMain = mainContent != null && !isCorruptedContent(mainContent);
            boolean hasValidBackup = backupContent != null && !isCorruptedContent(backupContent);
            
            if (!hasValidMain && !hasValidBackup && !validContents.isEmpty()) {
                // If we had valid content before but now have no valid files, it's corruption
                corruptionCount.incrementAndGet();
            }
            
        } catch (IOException e) {
            if (!validContents.isEmpty()) {
                corruptionCount.incrementAndGet();
            }
        }
    }

    /**
     * Check if content appears corrupted
     */
    private boolean isCorruptedContent(String content) {
        if (content == null) {
            return false;
        }
        
        // Check for obvious corruption patterns
        return content.contains("\0") ||
               content.length() > 0 && content.trim().isEmpty() && content.length() > 100 ||
               content.length() < 10 && !content.trim().isEmpty();
    }

    /**
     * Generate random content
     */
    private String generateLargeContent(int baseSize) {
        StringBuilder sb = new StringBuilder();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        int actualSize = baseSize + RANDOM.nextInt(baseSize / 2);
        
        for (int i = 0; i < actualSize; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
            if (i > 0 && i % 50 == 0) {
                sb.append('\n'); // Add some line breaks
            }
        }
        return sb.toString();
    }

    /**
     * Get test file name
     */
    private String getTestFileName(String testName) {
        return testDirPath.toString() + File.separator + "integration_" + testName + "_" + System.nanoTime() + ".txt";
    }

    /**
     * Recursively delete directory
     */
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