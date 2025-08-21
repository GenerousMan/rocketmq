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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Comprehensive reliability tests for MixAll file persistence logic.
 * Tests various scenarios including continuous writes, interruptions, and double file corruption.
 */
public class MixAllFilePersistenceReliabilityTest {

    private static final String TEST_DIR = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test-" + System.currentTimeMillis();
    private static final Random RANDOM = new Random();
    
    private Path testDirPath;

    @Before
    public void setUp() throws IOException {
        testDirPath = Paths.get(TEST_DIR);
        Files.createDirectories(testDirPath);
    }

    @After
    public void tearDown() throws IOException {
        if (Files.exists(testDirPath)) {
            deleteRecursively(testDirPath);
        }
    }

    /**
     * Test basic file persistence reliability with normal operations
     */
    @Test
    public void testBasicFilePersistenceReliability() throws IOException {
        String fileName = getTestFileName("basic");
        
        // Write initial content
        String content1 = "Initial content for basic test";
        MixAll.string2File(content1, fileName);
        
        // Verify content and backup
        assertThat(MixAll.file2String(fileName)).isEqualTo(content1);
        
        // Write updated content
        String content2 = "Updated content for basic test";
        MixAll.string2File(content2, fileName);
        
        // Verify both files exist and have correct content
        assertThat(MixAll.file2String(fileName)).isEqualTo(content2);
        assertThat(MixAll.file2String(fileName + ".bak")).isEqualTo(content1);
    }

    /**
     * Test continuous file writes to verify persistence stability
     */
    @Test
    public void testContinuousFilePersistence() throws IOException {
        String fileName = getTestFileName("continuous");
        int writeCount = 100;
        
        for (int i = 0; i < writeCount; i++) {
            String content = "Content iteration " + i + " - " + generateRandomContent(50);
            MixAll.string2File(content, fileName);
            
            // Verify content is correctly written
            String readContent = MixAll.file2String(fileName);
            assertThat(readContent).isEqualTo(content);
            
            // Verify backup exists (except for the first write)
            if (i > 0) {
                File backupFile = new File(fileName + ".bak");
                assertThat(backupFile.exists()).isTrue();
            }
        }
    }

    /**
     * Test file persistence with simulated interruptions (1000 iterations)
     * This test simulates crashes during write operations to detect double file corruption
     */
    @Test
    public void testFilePersistenceWithInterruptions() throws Exception {
        String fileName = getTestFileName("interruption");
        int iterationCount = 1000;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        AtomicInteger successfulWrites = new AtomicInteger(0);
        AtomicInteger corruptionDetected = new AtomicInteger(0);
        List<String> expectedContents = new ArrayList<>();
        
        try {
            for (int i = 0; i < iterationCount; i++) {
                final String content = "Iteration " + i + " - " + generateRandomContent(100);
                expectedContents.add(content);
                
                // Submit write task
                Future<?> writeTask = executor.submit(() -> {
                    try {
                        MixAll.string2File(content, fileName);
                        successfulWrites.incrementAndGet();
                    } catch (IOException e) {
                        // Expected for some interrupted operations
                    }
                });
                
                // Randomly interrupt some operations
                if (RANDOM.nextDouble() < 0.05) { // 5% chance of interruption
                    try {
                        writeTask.get(1, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        writeTask.cancel(true);
                    }
                } else {
                    writeTask.get();
                }
                
                // Verify file integrity after each operation
                verifyFileIntegrity(fileName, expectedContents, corruptionDetected);
            }
            
            // Final verification
            assertThat(corruptionDetected.get()).isEqualTo(0);
            assertThat(successfulWrites.get()).isGreaterThan((int)(iterationCount * 0.9)); // At least 90% success rate
            
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Test concurrent file writes to detect race conditions
     */
    @Test
    public void testConcurrentFilePersistence() throws Exception {
        String fileName = getTestFileName("concurrent");
        int threadCount = 5;
        int writesPerThread = 50;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        AtomicInteger totalWrites = new AtomicInteger(0);
        AtomicReference<Exception> exception = new AtomicReference<>();
        
        try {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < writesPerThread; i++) {
                            String content = "Thread-" + threadId + "-Write-" + i + "-" + generateRandomContent(30);
                            MixAll.string2File(content, fileName);
                            totalWrites.incrementAndGet();
                        }
                    } catch (Exception e) {
                        exception.set(e);
                    } finally {
                        finishLatch.countDown();
                    }
                });
            }
            
            startLatch.countDown();
            finishLatch.await(30, TimeUnit.SECONDS);
            
            if (exception.get() != null) {
                fail("Concurrent writes failed", exception.get());
            }
            
            // Verify final state
            String finalContent = MixAll.file2String(fileName);
            assertThat(finalContent).isNotNull();
            assertThat(finalContent).isNotEmpty();
            
            // Verify backup exists
            File backupFile = new File(fileName + ".bak");
            assertThat(backupFile.exists()).isTrue();
            
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Test file persistence with corrupted original file
     */
    @Test
    public void testPersistenceWithCorruptedOriginalFile() throws IOException {
        String fileName = getTestFileName("corrupted");
        
        // Create a corrupted file (empty or invalid)
        Files.write(Paths.get(fileName), new byte[0]);
        
        String validContent = "Valid content after corruption";
        MixAll.string2File(validContent, fileName);
        
        // Verify recovery
        assertThat(MixAll.file2String(fileName)).isEqualTo(validContent);
        assertThat(MixAll.file2String(fileName + ".bak")).isEqualTo("");
    }

    /**
     * Test large file persistence to verify performance and integrity
     */
    @Test
    public void testLargeFilePersistence() throws IOException {
        String fileName = getTestFileName("large");
        
        // Generate large content (1MB)
        StringBuilder largeContent = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeContent.append("Line ").append(i).append(" - ").append(generateRandomContent(100)).append("\n");
        }
        
        String content = largeContent.toString();
        long startTime = System.currentTimeMillis();
        
        MixAll.string2File(content, fileName);
        
        long endTime = System.currentTimeMillis();
        // Large file write completed successfully
        
        // Verify content integrity
        assertThat(MixAll.file2String(fileName)).isEqualTo(content);
        
        // Write second large content to test backup creation
        String content2 = largeContent.append("UPDATED").toString();
        MixAll.string2File(content2, fileName);
        
        assertThat(MixAll.file2String(fileName)).isEqualTo(content2);
        assertThat(MixAll.file2String(fileName + ".bak")).isEqualTo(content);
    }

    /**
     * Test edge cases for file persistence
     */
    @Test
    public void testEdgeCases() throws IOException {
        // Test empty content
        String fileName1 = getTestFileName("empty");
        MixAll.string2File("", fileName1);
        assertThat(MixAll.file2String(fileName1)).isEqualTo("");
        
        // Test null-like content
        String fileName2 = getTestFileName("null");
        MixAll.string2File("null", fileName2);
        assertThat(MixAll.file2String(fileName2)).isEqualTo("null");
        
        // Test special characters
        String fileName3 = getTestFileName("special");
        String specialContent = "Special: Chinese Test αβγ rocket \\n\\t\\r\\\"'\\\\";
        MixAll.string2File(specialContent, fileName3);
        assertThat(MixAll.file2String(fileName3)).isEqualTo(specialContent);
        
        // Test very long filename path
        StringBuilder longPathBuilder = new StringBuilder(testDirPath.toString());
        for (int i = 0; i < 20; i++) {
            longPathBuilder.append(File.separator).append("very");
        }
        for (int i = 0; i < 20; i++) {
            longPathBuilder.append(File.separator).append("long");
        }
        String longPath = longPathBuilder.append(".txt").toString();
        MixAll.string2File("Long path test", longPath);
        assertThat(MixAll.file2String(longPath)).isEqualTo("Long path test");
    }

    /**
     * Verify file integrity by checking if at least one valid file exists
     */
    private void verifyFileIntegrity(String fileName, List<String> expectedContents, AtomicInteger corruptionCount) {
        try {
            String mainContent = MixAll.file2String(fileName);
            String backupContent = MixAll.file2String(fileName + ".bak");
            
            // At least one file should exist and be non-null
            if (mainContent == null && backupContent == null) {
                corruptionCount.incrementAndGet();
                return;
            }
            
            // If main file exists, it should contain valid content
            if (mainContent != null && !isValidContent(mainContent, expectedContents)) {
                corruptionCount.incrementAndGet();
            }
            
            // If backup file exists, it should contain valid content
            if (backupContent != null && !isValidContent(backupContent, expectedContents)) {
                corruptionCount.incrementAndGet();
            }
            
        } catch (IOException e) {
            corruptionCount.incrementAndGet();
        }
    }

    /**
     * Check if content is valid (contains expected patterns)
     */
    private boolean isValidContent(String content, List<String> expectedContents) {
        if (content == null || content.trim().isEmpty()) {
            return expectedContents.isEmpty();
        }
        
        // Check if content matches any expected pattern
        return content.startsWith("Iteration") || expectedContents.contains(content);
    }

    /**
     * Generate random content of specified length
     */
    private String generateRandomContent(int length) {
        StringBuilder sb = new StringBuilder();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }

    /**
     * Get test file name with proper path
     */
    private String getTestFileName(String testName) {
        return testDirPath.toString() + File.separator + "test_" + testName + "_" + System.nanoTime() + ".txt";
    }

    /**
     * Recursively delete directory and all contents
     */
    private void deleteRecursively(Path path) throws IOException {
        if (Files.exists(path)) {
            if (Files.isDirectory(path)) {
                Files.list(path).forEach(child -> {
                    try {
                        deleteRecursively(child);
                    } catch (IOException e) {
                        // Ignore deletion errors in cleanup
                    }
                });
            }
            Files.deleteIfExists(path);
        }
    }
}