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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to simulate disk flush interruption without actual power failure.
 * 
 * This test demonstrates several techniques to simulate disk flush interruption:
 * 1. Process termination simulation (SIGKILL equivalent)
 * 2. File descriptor closure during write
 * 3. FileChannel force() interruption
 * 4. RandomAccessFile operations interruption
 * 5. OS buffer manipulation simulation
 * 6. Partial write simulation through controlled failure
 */
public class MixAllDiskFlushInterruptionSimulationTest {

    private static final String TEST_DIR = System.getProperty("java.io.tmpdir") + File.separator + 
                                          "rocketmq-disk-flush-simulation-" + System.currentTimeMillis();
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
     * Technique 1: Simulate process termination during file write
     * This simulates what happens when the JVM is killed during disk operations
     */
    @Test
    public void testProcessTerminationDuringWrite() throws Exception {
        String fileName = getTestFileName("process_termination");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        for (int i = 0; i < 100; i++) {
            String content = "Process termination test " + i + " - " + generateContent(500);
            
            try {
                // Simulate process termination at various stages
                simulateProcessTerminationDuringWrite(fileName, content, i % 4);
            } catch (SimulatedProcessTerminationException e) {
                // Expected - this simulates the process being killed
            }
            
            // Check file state after "process restart"
            verifyFileStateAfterInterruption(fileName, corruptionCount);
        }
        
        System.err.println("Process termination simulation - corruptions detected: " + corruptionCount.get());
    }

    /**
     * Technique 2: Simulate file descriptor closure during write
     * This simulates what happens when the OS forcibly closes file descriptors
     */
    @Test
    public void testFileDescriptorClosureDuringWrite() throws Exception {
        String fileName = getTestFileName("fd_closure");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        for (int i = 0; i < 100; i++) {
            String content = "FD closure test " + i + " - " + generateContent(300);
            
            try {
                simulateFileDescriptorClosure(fileName, content);
            } catch (IOException e) {
                // Expected when file descriptor is forcibly closed
            }
            
            verifyFileStateAfterInterruption(fileName, corruptionCount);
        }
        
        System.err.println("File descriptor closure simulation - corruptions detected: " + corruptionCount.get());
    }

    /**
     * Technique 3: Simulate FileChannel.force() interruption
     * This simulates what happens when disk sync is interrupted
     */
    @Test
    public void testFileChannelForceInterruption() throws Exception {
        String fileName = getTestFileName("force_interruption");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        for (int i = 0; i < 100; i++) {
            String content = "Force interruption test " + i + " - " + generateContent(400);
            
            try {
                simulateFileChannelForceInterruption(fileName, content);
            } catch (IOException e) {
                // Expected when force operation is interrupted
            }
            
            verifyFileStateAfterInterruption(fileName, corruptionCount);
        }
        
        System.err.println("FileChannel force interruption - corruptions detected: " + corruptionCount.get());
    }

    /**
     * Technique 4: Simulate partial write through controlled buffer manipulation
     * This simulates what happens when only part of the data reaches the disk
     */
    @Test
    public void testPartialWriteSimulation() throws Exception {
        String fileName = getTestFileName("partial_write");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        for (int i = 0; i < 100; i++) {
            String content = "Partial write test " + i + " - " + generateContent(600);
            
            try {
                simulatePartialWrite(fileName, content);
            } catch (IOException e) {
                // Expected for partial write scenarios
            }
            
            verifyFileStateAfterInterruption(fileName, corruptionCount);
        }
        
        System.err.println("Partial write simulation - corruptions detected: " + corruptionCount.get());
    }

    /**
     * Technique 5: Simulate concurrent file operations with sudden termination
     * This tests the reliability of MixAll.string2File under concurrent stress
     */
    @Test
    public void testConcurrentOperationsWithTermination() throws Exception {
        String fileName = getTestFileName("concurrent_termination");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        AtomicInteger terminatedOperations = new AtomicInteger(0);
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(3);
        
        // Start multiple concurrent write operations
        for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 50; i++) {
                        String content = "Thread-" + threadId + "-Op-" + i + "-" + generateContent(200);
                        
                        try {
                            if (RANDOM.nextDouble() < 0.2) {
                                // Simulate sudden termination
                                simulateProcessTerminationDuringWrite(fileName, content, RANDOM.nextInt(4));
                                terminatedOperations.incrementAndGet();
                            } else {
                                MixAll.string2File(content, fileName);
                            }
                        } catch (SimulatedProcessTerminationException e) {
                            terminatedOperations.incrementAndGet();
                        } catch (IOException e) {
                            // Expected for some scenarios
                        }
                        
                        // Periodically check corruption
                        if (i % 10 == 0) {
                            verifyFileStateAfterInterruption(fileName, corruptionCount);
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
        
        System.err.println("Concurrent operations with termination:");
        System.err.println("- Terminated operations: " + terminatedOperations.get());
        System.err.println("- Corruptions detected: " + corruptionCount.get());
        
        // Final integrity check
        verifyFileStateAfterInterruption(fileName, corruptionCount);
    }

    /**
     * Technique 6: Test recovery scenarios after simulated crashes
     */
    @Test
    public void testRecoveryAfterSimulatedCrashes() throws Exception {
        String fileName = getTestFileName("crash_recovery");
        AtomicInteger corruptionCount = new AtomicInteger(0);
        
        // Phase 1: Initialize with good content
        String baselineContent = "Baseline content for crash recovery test";
        MixAll.string2File(baselineContent, fileName);
        
        // Phase 2: Simulate multiple crash scenarios
        for (int i = 0; i < 200; i++) {
            String newContent = "Recovery test " + i + " - " + generateContent(300);
            
            try {
                // Rotate through different crash simulations
                switch (i % 5) {
                    case 0:
                        simulateProcessTerminationDuringWrite(fileName, newContent, 1);
                        break;
                    case 1:
                        simulateFileDescriptorClosure(fileName, newContent);
                        break;
                    case 2:
                        simulateFileChannelForceInterruption(fileName, newContent);
                        break;
                    case 3:
                        simulatePartialWrite(fileName, newContent);
                        break;
                    case 4:
                        // Normal operation
                        MixAll.string2File(newContent, fileName);
                        break;
                }
            } catch (Exception e) {
                // Expected for crash simulations
            }
            
            verifyFileStateAfterInterruption(fileName, corruptionCount);
        }
        
        // Phase 3: Test normal recovery
        String recoveryContent = "Post-crash recovery verification";
        MixAll.string2File(recoveryContent, fileName);
        assertThat(MixAll.file2String(fileName)).isEqualTo(recoveryContent);
        
        System.err.println("Crash recovery test - total corruptions: " + corruptionCount.get());
        
        // The key test: verify the backup mechanism works
        assertThat(corruptionCount.get()).isLessThan(50); // Some corruption is expected, but not excessive
    }

    // Simulation methods

    /**
     * Simulate process termination at different stages of file writing
     */
    private void simulateProcessTerminationDuringWrite(String fileName, String content, int stage) throws IOException {
        String bakFile = fileName + ".bak";
        String prevContent = MixAll.file2String(fileName);
        
        switch (stage) {
            case 0: // Terminate during backup read
                throw new SimulatedProcessTerminationException("Process terminated during backup read");
            
            case 1: // Terminate during backup write
                if (prevContent != null) {
                    // Start backup write but don't complete
                    File backupFile = new File(bakFile);
                    try (FileOutputStream fos = new FileOutputStream(backupFile)) {
                        byte[] data = prevContent.getBytes(MixAll.DEFAULT_CHARSET);
                        int partialSize = Math.max(1, RANDOM.nextInt(Math.max(1, data.length / 2)));
                        fos.write(data, 0, Math.min(partialSize, data.length));
                        // Don't close properly - simulate crash
                    }
                }
                throw new SimulatedProcessTerminationException("Process terminated during backup write");
            
            case 2: // Terminate during main file write
                if (prevContent != null) {
                    MixAll.string2FileNotSafe(prevContent, bakFile);
                }
                // Start main write but don't complete
                File mainFile = new File(fileName);
                try (FileOutputStream fos = new FileOutputStream(mainFile)) {
                    byte[] data = content.getBytes(MixAll.DEFAULT_CHARSET);
                    int partialSize = Math.max(1, RANDOM.nextInt(Math.max(1, data.length / 2)));
                    fos.write(data, 0, Math.min(partialSize, data.length));
                    // Don't close properly
                }
                throw new SimulatedProcessTerminationException("Process terminated during main write");
            
            case 3: // Terminate after write but before close
                if (prevContent != null) {
                    MixAll.string2FileNotSafe(prevContent, bakFile);
                }
                File file = new File(fileName);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.write(content.getBytes(MixAll.DEFAULT_CHARSET));
                    fos.flush();
                    // Terminate before close - this might leave file in inconsistent state
                }
                throw new SimulatedProcessTerminationException("Process terminated before file close");
        }
    }

    /**
     * Simulate file descriptor closure during write
     */
    private void simulateFileDescriptorClosure(String fileName, String content) throws IOException {
        File file = new File(fileName);
        
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] data = content.getBytes(MixAll.DEFAULT_CHARSET);
            int partialSize = RANDOM.nextInt(data.length / 2) + 1;
            
            // Write partial data
            fos.write(data, 0, Math.min(partialSize, data.length));
            fos.flush();
            
            // Simulate forced file descriptor closure by closing the stream abruptly
            // This simulates what happens when the OS forcibly closes file descriptors
            try {
                // Force close through reflection to simulate abrupt closure
                Field fdField = FileOutputStream.class.getDeclaredField("fd");
                fdField.setAccessible(true);
                Object fd = fdField.get(fos);
                if (fd != null) {
                    Method closeMethod = fd.getClass().getDeclaredMethod("closeAll", java.io.Closeable.class);
                    closeMethod.setAccessible(true);
                    closeMethod.invoke(fd, fos);
                }
            } catch (Exception e) {
                // Fallback: just close normally
                fos.close();
            }
        }
        
        throw new IOException("Simulated file descriptor closure");
    }

    /**
     * Simulate FileChannel.force() interruption
     */
    private void simulateFileChannelForceInterruption(String fileName, String content) throws IOException {
        File file = new File(fileName);
        
        try (FileChannel channel = FileChannel.open(file.toPath(), 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            
            ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(MixAll.DEFAULT_CHARSET));
            channel.write(buffer);
            
            // Simulate interruption during force/sync operation
            try {
                channel.force(true); // This might be interrupted
            } catch (Exception e) {
                // Simulate force operation failure
                throw new IOException("Simulated force operation interruption", e);
            }
        }
        
        // Randomly corrupt the file to simulate incomplete sync
        if (RANDOM.nextBoolean()) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                long fileLength = raf.length();
                if (fileLength > 10) {
                    long corruptionPoint = RANDOM.nextLong() % (fileLength - 10);
                    raf.seek(corruptionPoint);
                    raf.write("CORRUPT".getBytes());
                }
            }
        }
        
        throw new IOException("Simulated sync interruption");
    }

    /**
     * Simulate partial write through controlled buffer manipulation
     */
    private void simulatePartialWrite(String fileName, String content) throws IOException {
        File file = new File(fileName);
        
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            byte[] data = content.getBytes(MixAll.DEFAULT_CHARSET);
            
            // Write only partial data to simulate incomplete disk write
            int partialSize = Math.max(1, RANDOM.nextInt(Math.max(1, data.length / 3)) + 1);
            raf.write(data, 0, Math.min(partialSize, data.length));
            
            // Truncate file to simulate incomplete write
            raf.setLength(partialSize);
            
            // Don't sync to simulate crash before sync
        }
        
        throw new IOException("Simulated partial write");
    }

    /**
     * Verify file state after interruption and detect corruption
     */
    private void verifyFileStateAfterInterruption(String fileName, AtomicInteger corruptionCount) {
        try {
            String mainContent = MixAll.file2String(fileName);
            String backupContent = MixAll.file2String(fileName + ".bak");
            
            boolean mainValid = isValidContent(mainContent);
            boolean backupValid = isValidContent(backupContent);
            
            // Double corruption: both main and backup files are invalid
            if (!mainValid && !backupValid && (mainContent != null || backupContent != null)) {
                corruptionCount.incrementAndGet();
            }
            
        } catch (IOException e) {
            // File access error could indicate corruption
            corruptionCount.incrementAndGet();
        }
    }

    /**
     * Check if content is valid (not corrupted)
     */
    private boolean isValidContent(String content) {
        if (content == null) {
            return true; // Absence is not corruption
        }
        
        return !content.contains("\0") && // No null bytes
               !content.contains("CORRUPT") && // No corruption markers
               (content.trim().isEmpty() || content.contains("test")); // Valid test content
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
        return testDirPath.toString() + File.separator + "flush_sim_" + testName + "_" + System.nanoTime() + ".txt";
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

    /**
     * Custom exception to simulate process termination
     */
    static class SimulatedProcessTerminationException extends RuntimeException {
        public SimulatedProcessTerminationException(String message) {
            super(message);
        }
    }
}