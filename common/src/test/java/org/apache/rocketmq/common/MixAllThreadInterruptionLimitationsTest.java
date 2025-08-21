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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to demonstrate the limitations of Thread.isInterrupted() for interrupting disk I/O operations.
 * 
 * This test shows that:
 * 1. Thread.isInterrupted() cannot interrupt native I/O operations in progress
 * 2. Partial writes can occur when interruption happens between operations
 * 3. File corruption can result from improper interruption handling
 */
public class MixAllThreadInterruptionLimitationsTest {

    private static final String TEST_DIR = System.getProperty("java.io.tmpdir") + File.separator + 
                                          "rocketmq-interruption-limitations-" + System.currentTimeMillis();
    
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
     * Test 1: Demonstrate that Thread.isInterrupted() cannot interrupt native I/O operations
     */
    @Test
    public void testThreadInterruptionCannotStopNativeIO() throws Exception {
        String fileName = getTestFileName("native_io_test");
        
        // Create large content to ensure longer I/O operation
        StringBuilder largeContent = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            largeContent.append("This is line ").append(i).append(" of large content for testing native I/O interruption.\n");
        }
        String content = largeContent.toString();
        
        AtomicBoolean ioCompleted = new AtomicBoolean(false);
        AtomicBoolean threadWasInterrupted = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        
        CountDownLatch ioStarted = new CountDownLatch(1);
        
        Future<?> writeTask = executorService.submit(() -> {
            try {
                File file = new File(fileName);
                ioStarted.countDown();
                
                // Simulate the MixAll.string2FileNotSafe operation
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    byte[] data = content.getBytes(MixAll.DEFAULT_CHARSET);
                    
                    // Write in chunks and check for interruption between chunks
                    int chunkSize = 1024;
                    for (int i = 0; i < data.length; i += chunkSize) {
                        // Check for interruption BEFORE I/O operation
                        if (Thread.currentThread().isInterrupted()) {
                            threadWasInterrupted.set(true);
                            throw new InterruptedException("Thread was interrupted");
                        }
                        
                        int end = Math.min(i + chunkSize, data.length);
                        // This native I/O operation CANNOT be interrupted by Thread.interrupt()
                        fos.write(data, i, end - i);
                        fos.flush();
                        
                        // Small delay to make interruption more likely to occur during I/O
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            threadWasInterrupted.set(true);
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                    }
                }
                
                ioCompleted.set(true);
                
            } catch (Exception e) {
                exception.set(e);
            }
        });
        
        // Wait for I/O to start
        ioStarted.await();
        
        // Interrupt the thread while I/O is in progress
        Thread.sleep(50); // Let some I/O happen first
        writeTask.cancel(true);
        
        try {
            writeTask.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Expected - task was cancelled
        }
        
        // Analyze results
        File resultFile = new File(fileName);
        boolean fileExists = resultFile.exists();
        long fileSize = fileExists ? resultFile.length() : 0;
        
        System.err.println("=== Native I/O Interruption Test Results ===");
        System.err.println("I/O completed: " + ioCompleted.get());
        System.err.println("Thread was interrupted: " + threadWasInterrupted.get());
        System.err.println("File exists: " + fileExists);
        System.err.println("File size: " + fileSize + " bytes");
        System.err.println("Expected size: " + content.getBytes().length + " bytes");
        
        if (fileExists && fileSize > 0 && fileSize < content.getBytes().length) {
            System.err.println("WARNING: Partial file written - this demonstrates the risk!");
        }
    }

    /**
     * Test 2: Demonstrate how interruption during MixAll.string2File can create inconsistent state
     */
    @Test
    public void testInterruptionDuringString2FileCanCauseInconsistency() throws Exception {
        String fileName = getTestFileName("string2file_interruption");
        
        // Initialize with valid content
        String initialContent = "Initial content that should be preserved";
        MixAll.string2File(initialContent, fileName);
        assertThat(MixAll.file2String(fileName)).isEqualTo(initialContent);
        
        AtomicInteger inconsistentStates = new AtomicInteger(0);
        AtomicInteger totalAttempts = new AtomicInteger(0);
        
        // Try multiple interruption scenarios
        for (int i = 0; i < 100; i++) {
            final String newContent = "New content attempt " + i;
            totalAttempts.incrementAndGet();
            
            Future<?> writeTask = executorService.submit(() -> {
                try {
                    // Use a custom interruptible version to demonstrate the problem
                    interruptibleString2File(newContent, fileName);
                } catch (Exception e) {
                    // Expected for interruptions
                }
            });
            
            // Randomly interrupt some operations
            if (i % 3 == 0) {
                Thread.sleep(1); // Let operation start
                writeTask.cancel(true);
            } else {
                writeTask.get(1, TimeUnit.SECONDS);
            }
            
            // Check for inconsistent state
            if (isInconsistentState(fileName, newContent, initialContent)) {
                inconsistentStates.incrementAndGet();
            }
        }
        
        System.err.println("=== String2File Interruption Test Results ===");
        System.err.println("Total attempts: " + totalAttempts.get());
        System.err.println("Inconsistent states detected: " + inconsistentStates.get());
        System.err.println("Inconsistency rate: " + String.format("%.2f%%", 
            (inconsistentStates.get() * 100.0 / totalAttempts.get())));
        
        // Any inconsistent state indicates a reliability problem
        if (inconsistentStates.get() > 0) {
            System.err.println("WARNING: Thread interruption can cause file inconsistencies!");
        }
    }

    /**
     * Test 3: Compare with atomic operations
     */
    @Test
    public void testAtomicVsInterruptibleOperations() throws Exception {
        String atomicFileName = getTestFileName("atomic_operations");
        String interruptibleFileName = getTestFileName("interruptible_operations");
        
        int testIterations = 50;
        AtomicInteger atomicFailures = new AtomicInteger(0);
        AtomicInteger interruptibleFailures = new AtomicInteger(0);
        
        // Test atomic operations (using synchronized MixAll.string2File)
        for (int i = 0; i < testIterations; i++) {
            final String content = "Atomic test " + i;
            
            Future<?> atomicTask = executorService.submit(() -> {
                try {
                    synchronized (this) {
                        MixAll.string2File(content, atomicFileName);
                    }
                } catch (IOException e) {
                    atomicFailures.incrementAndGet();
                }
            });
            
            if (i % 5 == 0) {
                Thread.sleep(1);
                atomicTask.cancel(true);
            } else {
                atomicTask.get();
            }
            
            // Verify integrity
            try {
                String result = MixAll.file2String(atomicFileName);
                if (result != null && !result.startsWith("Atomic test")) {
                    atomicFailures.incrementAndGet();
                }
            } catch (IOException e) {
                atomicFailures.incrementAndGet();
            }
        }
        
        // Test interruptible operations
        for (int i = 0; i < testIterations; i++) {
            final String content = "Interruptible test " + i;
            
            Future<?> interruptibleTask = executorService.submit(() -> {
                try {
                    interruptibleString2File(content, interruptibleFileName);
                } catch (Exception e) {
                    // Expected for interruptions
                }
            });
            
            if (i % 5 == 0) {
                Thread.sleep(1);
                interruptibleTask.cancel(true);
            } else {
                try {
                    interruptibleTask.get();
                } catch (Exception e) {
                    // Expected
                }
            }
            
            // Check for corruption
            if (isFileCorrupted(interruptibleFileName)) {
                interruptibleFailures.incrementAndGet();
            }
        }
        
        System.err.println("=== Atomic vs Interruptible Operations ===");
        System.err.println("Atomic operation failures: " + atomicFailures.get());
        System.err.println("Interruptible operation failures: " + interruptibleFailures.get());
        
        // Atomic operations should be more reliable
        assertThat(atomicFailures.get()).isLessThan(interruptibleFailures.get() + 1);
    }

    /**
     * Custom interruptible string2File that demonstrates the problem
     */
    private void interruptibleString2File(String str, String fileName) throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted before operation");
        }
        
        String bakFile = fileName + ".bak";
        String prevContent = MixAll.file2String(fileName);
        
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted during backup read");
        }
        
        if (prevContent != null) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread was interrupted before backup write");
            }
            // This could be interrupted, leaving backup in inconsistent state
            MixAll.string2FileNotSafe(prevContent, bakFile);
        }
        
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted before main write");
        }
        
        // This write could be interrupted, leaving main file corrupted
        // but the interruption check cannot stop the native I/O once it starts
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] data = str.getBytes(MixAll.DEFAULT_CHARSET);
            // The actual write operation cannot be interrupted by Thread.interrupt()
            fos.write(data);
            fos.flush();
        }
    }

    /**
     * Check if files are in inconsistent state
     */
    private boolean isInconsistentState(String fileName, String expectedNew, String expectedOld) {
        try {
            String mainContent = MixAll.file2String(fileName);
            String backupContent = MixAll.file2String(fileName + ".bak");
            
            // Inconsistent if both files exist but neither contains expected content
            boolean mainValid = mainContent != null && 
                (mainContent.equals(expectedNew) || mainContent.equals(expectedOld) || mainContent.startsWith("Initial"));
            boolean backupValid = backupContent == null || 
                backupContent.equals(expectedOld) || backupContent.startsWith("Initial");
            
            return !mainValid || !backupValid;
            
        } catch (IOException e) {
            return true; // I/O error indicates inconsistent state
        }
    }

    /**
     * Check if file is corrupted
     */
    private boolean isFileCorrupted(String fileName) {
        try {
            String content = MixAll.file2String(fileName);
            if (content == null) {
                return false; // Missing file is not corruption, just no data
            }
            
            // Check for corruption indicators
            return content.contains("\0") || // Null bytes
                   content.length() > 0 && content.trim().isEmpty() || // Suspicious empty content
                   content.length() < 10 && !content.trim().isEmpty(); // Suspiciously short
                   
        } catch (IOException e) {
            return true; // Cannot read file indicates corruption
        }
    }

    private String getTestFileName(String testName) {
        return testDirPath.toString() + File.separator + "interruption_" + testName + "_" + System.nanoTime() + ".txt";
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