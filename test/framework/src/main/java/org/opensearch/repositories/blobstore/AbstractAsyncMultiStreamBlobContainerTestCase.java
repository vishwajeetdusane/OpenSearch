/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.write.StreamContextSupplier;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provider-neutral contract tests for {@link AsyncMultiStreamBlobContainer#asyncBlobUpload}.
 *
 * Providers supply a test-only harness around their production container. The shared tests own the source streams,
 * listener, finalizer, and assertions so provider-specific wire mechanics do not define the contract.
 */
public abstract class AbstractAsyncMultiStreamBlobContainerTestCase extends OpenSearchTestCase {

    private static final long AWAIT_TIMEOUT_SECONDS = 30;

    private AsyncUploadTestHarness harness;

    protected abstract AsyncUploadTestHarness createTestHarness() throws Exception;

    protected Set<Deviation> deviations() {
        return Set.of();
    }

    protected WritePriority writePriority() {
        return WritePriority.NORMAL;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        harness = createTestHarness();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            if (harness != null) {
                harness.close();
            }
        } finally {
            super.tearDown();
        }
    }

    public void testMultipartUploadPublishesExactBytesAndClosesStreams() throws Exception {
        final byte[] contents = multipartContents("exact-bytes");
        final AtomicInteger finalizerCalls = new AtomicInteger();
        final AtomicReference<Boolean> finalizerResult = new AtomicReference<>();

        final UploadExecution execution = startUpload("exact-bytes", contents, false, Map.of(), uploadSuccessful -> {
            finalizerCalls.incrementAndGet();
            finalizerResult.set(uploadSuccessful);
        });

        assertSuccessfulUpload(execution, true);
        assertEquals(1, finalizerCalls.get());
        assertEquals(Boolean.TRUE, finalizerResult.get());
        assertStoredBlob("exact-bytes", contents, Map.of());
    }

    public void testOutOfOrderPartCompletionPreservesLogicalOrderAndMetadata() throws Exception {
        final byte[] contents = multipartContents("out-of-order");
        final Map<String, String> metadata = Map.of("contract-key", "contract-value", "generation", "7");
        harness.setPartCompletionOrder(PartCompletionOrder.REVERSE);

        final UploadExecution execution = startUpload("out-of-order", contents, false, metadata, uploadSuccessful -> {});

        assertSuccessfulUpload(execution, true);
        assertStoredBlob("out-of-order", contents, metadata);

        final List<Integer> expectedCompletionOrder = new ArrayList<>();
        for (int part = execution.source.numberOfParts() - 1; part >= 0; part--) {
            expectedCompletionOrder.add(part);
        }
        assertEquals(expectedCompletionOrder, harness.completedPartOrder());
    }

    public void testSuccessWaitsForFinalPublicationAcknowledgement() throws Exception {
        final byte[] contents = multipartContents("publication-gate");
        harness.holdFinalPublication();

        final UploadExecution execution = startUpload("publication-gate", contents, false, Map.of(), uploadSuccessful -> {});

        harness.awaitFinalPublicationAttempt();
        assertEquals(0, execution.listener.terminalEventCount());
        assertNull(harness.getBlob("publication-gate"));

        harness.releaseFinalPublication();
        assertSuccessfulUpload(execution, true);
        assertStoredBlob("publication-gate", contents, Map.of());
    }

    public void testFinalizerFailurePreventsMultipartPublication() throws Exception {
        final byte[] contents = multipartContents("finalizer-failure");
        final AtomicInteger finalizerCalls = new AtomicInteger();
        final AtomicReference<Boolean> finalizerResult = new AtomicReference<>();

        final UploadExecution execution = startUpload("finalizer-failure", contents, false, Map.of(), uploadSuccessful -> {
            finalizerCalls.incrementAndGet();
            finalizerResult.set(uploadSuccessful);
            throw new IOException("simulated finalizer failure");
        });

        assertFailedUpload(execution, true);
        assertEquals(1, finalizerCalls.get());
        assertEquals(Boolean.TRUE, finalizerResult.get());
        assertNull(harness.getBlob("finalizer-failure"));
        harness.assertNoDanglingUploadState();
    }

    public void testPartFailureLeavesNoPartialVisibleObject() throws Exception {
        final byte[] contents = multipartContents("part-failure");
        final AtomicInteger finalizerCalls = new AtomicInteger();
        harness.failNextUpload(FailureStage.PART_UPLOAD);

        final UploadExecution execution = startUpload(
            "part-failure",
            contents,
            false,
            Map.of(),
            uploadSuccessful -> finalizerCalls.incrementAndGet()
        );

        assertFailedUpload(execution, true);
        assertEquals(0, finalizerCalls.get());
        assertNull(harness.getBlob("part-failure"));
        harness.assertNoDanglingUploadState();
    }

    public void testFailedFinalPublicationPreservesExistingObject() throws Exception {
        final byte[] original = singlePartContents("existing-before-final-failure");
        final Map<String, String> originalMetadata = Map.of("version", "original");
        final byte[] replacement = multipartContents("failed-replacement");
        final AtomicInteger finalizerCalls = new AtomicInteger();
        harness.putBlob("failed-replacement", original, originalMetadata);
        harness.failNextUpload(FailureStage.FINAL_PUBLICATION);

        final UploadExecution execution = startUpload(
            "failed-replacement",
            replacement,
            false,
            Map.of("version", "replacement"),
            uploadSuccessful -> finalizerCalls.incrementAndGet()
        );

        assertFailedUpload(execution, true);
        assertEquals(1, finalizerCalls.get());
        assertStoredBlob("failed-replacement", original, originalMetadata);
        harness.assertNoDanglingUploadState();
    }

    public void testOverwriteReplacesExistingObjectAndMetadata() throws Exception {
        final byte[] original = singlePartContents("overwrite-original");
        final byte[] replacement = multipartContents("overwrite-replacement");
        harness.putBlob("overwrite", original, Map.of("version", "original"));

        final UploadExecution execution = startUpload(
            "overwrite",
            replacement,
            false,
            Map.of("version", "replacement"),
            uploadSuccessful -> {}
        );

        assertSuccessfulUpload(execution, true);
        assertStoredBlob("overwrite", replacement, Map.of("version", "replacement"));
    }

    public void testCreateOnlyExistingObjectBehaviorIsDeclared() throws Exception {
        final byte[] original = singlePartContents("create-only-original");
        final Map<String, String> originalMetadata = Map.of("version", "original");
        final byte[] replacement = multipartContents("create-only-replacement");
        final Map<String, String> replacementMetadata = Map.of("version", "replacement");
        harness.putBlob("create-only", original, originalMetadata);

        final UploadExecution execution = startUpload("create-only", replacement, true, replacementMetadata, uploadSuccessful -> {});

        if (deviations().contains(Deviation.FAIL_IF_ALREADY_EXISTS_IGNORED)) {
            assertSuccessfulUpload(execution, true);
            assertStoredBlob("create-only", replacement, replacementMetadata);
        } else {
            assertFailedUpload(execution, false);
            assertStoredBlob("create-only", original, originalMetadata);
        }
    }

    public void testSinglePartFinalizerFailureBehaviorIsDeclared() throws Exception {
        final byte[] original = singlePartContents("single-original");
        final Map<String, String> originalMetadata = Map.of("version", "original");
        final byte[] replacement = singlePartContents("single-replacement");
        harness.putBlob("single-finalizer-failure", original, originalMetadata);

        final UploadExecution execution = startUpload(
            "single-finalizer-failure",
            replacement,
            false,
            Map.of("version", "replacement"),
            uploadSuccessful -> {
                throw new IOException("simulated finalizer failure");
            }
        );

        assertFailedUpload(execution, true);
        if (deviations().contains(Deviation.SINGLE_PART_FINALIZER_FAILURE_DELETES_EXISTING_OBJECT)) {
            assertNull(harness.getBlob("single-finalizer-failure"));
        } else {
            assertStoredBlob("single-finalizer-failure", original, originalMetadata);
        }
    }

    private UploadExecution startUpload(
        String blobName,
        byte[] contents,
        boolean failIfAlreadyExists,
        Map<String, String> metadata,
        org.opensearch.common.CheckedConsumer<Boolean, IOException> uploadFinalizer
    ) {
        final TrackingSource source = new TrackingSource(contents, harness::setExpectedPartCount);
        final WriteContext writeContext = new WriteContext.Builder().fileName(blobName)
            .streamContextSupplier(source.streamContextSupplier())
            .fileSize(contents.length)
            .failIfAlreadyExists(failIfAlreadyExists)
            .writePriority(writePriority())
            .uploadFinalizer(uploadFinalizer)
            .doRemoteDataIntegrityCheck(false)
            .metadata(metadata)
            .build();
        final TerminalListener listener = new TerminalListener();
        IOException synchronousFailure = null;

        assertEquals("provideStream must remain lazy until upload initiation", 0, source.providedStreamCount());
        try {
            harness.container().asyncBlobUpload(writeContext, listener);
        } catch (IOException e) {
            synchronousFailure = e;
        }
        return new UploadExecution(source, listener, synchronousFailure);
    }

    private void assertSuccessfulUpload(UploadExecution execution, boolean expectFullSourceConsumption) throws Exception {
        assertNull("upload initiation failed synchronously", execution.synchronousFailure);
        execution.listener.awaitTerminalEvent();
        harness.awaitIdle();
        execution.listener.assertSingleSuccess();
        if (expectFullSourceConsumption) {
            execution.source.assertFullyConsumedAndClosed();
        } else {
            execution.source.assertOpenedStreamsClosed();
        }
    }

    private void assertFailedUpload(UploadExecution execution, boolean expectFullSourceConsumption) throws Exception {
        if (execution.synchronousFailure == null) {
            execution.listener.awaitTerminalEvent();
        }
        harness.awaitIdle();
        if (execution.synchronousFailure == null) {
            execution.listener.assertSingleFailure();
        } else {
            assertEquals("a synchronous failure must not also notify the listener", 0, execution.listener.terminalEventCount());
        }
        if (expectFullSourceConsumption) {
            execution.source.assertFullyConsumedAndClosed();
        } else {
            execution.source.assertOpenedStreamsClosed();
        }
    }

    private void assertStoredBlob(String blobName, byte[] expectedContents, Map<String, String> expectedMetadata) {
        final StoredBlob storedBlob = harness.getBlob(blobName);
        assertNotNull("expected visible blob [" + blobName + "]", storedBlob);
        assertBytesEqual(expectedContents, storedBlob.contents());
        assertEquals(expectedMetadata, storedBlob.metadata());
    }

    private byte[] multipartContents(String salt) {
        return deterministicContents(Math.toIntExact(harness.multipartUploadSize()), salt.hashCode());
    }

    private byte[] singlePartContents(String salt) {
        return deterministicContents(Math.toIntExact(harness.singleUploadSize()), salt.hashCode());
    }

    private static byte[] deterministicContents(int length, int seed) {
        final byte[] contents = new byte[length];
        int value = seed;
        for (int index = 0; index < contents.length; index++) {
            value = (value * 31) + index;
            contents[index] = (byte) (value ^ (value >>> 16));
        }
        return contents;
    }

    private static void assertBytesEqual(byte[] expected, byte[] actual) {
        assertEquals("content length", expected.length, actual.length);
        for (int index = 0; index < expected.length; index++) {
            if (expected[index] != actual[index]) {
                fail(
                    "content differs at offset ["
                        + index
                        + "], expected ["
                        + Byte.toUnsignedInt(expected[index])
                        + "] but was ["
                        + Byte.toUnsignedInt(actual[index])
                        + "]"
                );
            }
        }
    }

    protected enum Deviation {
        FAIL_IF_ALREADY_EXISTS_IGNORED,
        SINGLE_PART_FINALIZER_FAILURE_DELETES_EXISTING_OBJECT
    }

    protected enum FailureStage {
        PART_UPLOAD,
        FINAL_PUBLICATION
    }

    protected enum PartCompletionOrder {
        NATURAL,
        REVERSE
    }

    protected interface AsyncUploadTestHarness extends Closeable {
        AsyncMultiStreamBlobContainer container();

        long multipartUploadSize();

        long singleUploadSize();

        StoredBlob getBlob(String blobName);

        void putBlob(String blobName, byte[] contents, Map<String, String> metadata);

        void setExpectedPartCount(int partCount);

        void setPartCompletionOrder(PartCompletionOrder completionOrder);

        List<Integer> completedPartOrder();

        void failNextUpload(FailureStage failureStage);

        void holdFinalPublication();

        void awaitFinalPublicationAttempt() throws Exception;

        void releaseFinalPublication();

        void assertNoDanglingUploadState();

        void awaitIdle() throws Exception;
    }

    protected static final class StoredBlob {
        private final byte[] contents;
        private final Map<String, String> metadata;

        public StoredBlob(byte[] contents, Map<String, String> metadata) {
            this.contents = Arrays.copyOf(contents, contents.length);
            this.metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }

        public byte[] contents() {
            return Arrays.copyOf(contents, contents.length);
        }

        public Map<String, String> metadata() {
            return metadata;
        }
    }

    private static final class UploadExecution {
        private final TrackingSource source;
        private final TerminalListener listener;
        private final IOException synchronousFailure;

        private UploadExecution(TrackingSource source, TerminalListener listener, IOException synchronousFailure) {
            this.source = source;
            this.listener = listener;
            this.synchronousFailure = synchronousFailure;
        }
    }

    private static final class TerminalListener implements ActionListener<Void> {
        private final AtomicInteger responses = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final CountDownLatch terminalEvent = new CountDownLatch(1);

        @Override
        public void onResponse(Void unused) {
            responses.incrementAndGet();
            terminalEvent.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            failure.compareAndSet(null, e);
            failures.incrementAndGet();
            terminalEvent.countDown();
        }

        private void awaitTerminalEvent() throws InterruptedException {
            assertTrue("timed out waiting for upload completion", terminalEvent.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        private int terminalEventCount() {
            return responses.get() + failures.get();
        }

        private void assertSingleSuccess() {
            assertEquals(1, responses.get());
            assertEquals(0, failures.get());
            assertNull(failure.get());
        }

        private void assertSingleFailure() {
            assertEquals(0, responses.get());
            assertEquals(1, failures.get());
            assertNotNull(failure.get());
        }
    }

    private static final class TrackingSource {
        private final byte[] contents;
        private final java.util.function.IntConsumer partCountConsumer;
        private final AtomicInteger streamContextCalls = new AtomicInteger();
        private final ConcurrentHashMap<Integer, TrackedPart> parts = new ConcurrentHashMap<>();
        private volatile int numberOfParts;

        private TrackingSource(byte[] contents, java.util.function.IntConsumer partCountConsumer) {
            this.contents = Arrays.copyOf(contents, contents.length);
            this.partCountConsumer = partCountConsumer;
        }

        private StreamContextSupplier streamContextSupplier() {
            return partSize -> {
                if (streamContextCalls.incrementAndGet() != 1) {
                    throw new IllegalStateException("stream context requested more than once");
                }
                if (partSize <= 0) {
                    throw new IllegalArgumentException("part size must be positive");
                }
                numberOfParts = Math.toIntExact((contents.length + partSize - 1) / partSize);
                final long lastPartSize = contents.length - (partSize * (numberOfParts - 1L));
                partCountConsumer.accept(numberOfParts);
                return new StreamContext(
                    (partNumber, size, position) -> provideStream(partNumber, size, position, partSize),
                    partSize,
                    lastPartSize,
                    numberOfParts
                );
            };
        }

        private InputStreamContainer provideStream(int partNumber, long size, long position, long partSize) throws IOException {
            if (partNumber < 0 || partNumber >= numberOfParts) {
                throw new IOException("invalid zero-based part number [" + partNumber + "]");
            }
            final long expectedPosition = partSize * partNumber;
            final long expectedSize = partNumber == numberOfParts - 1 ? contents.length - expectedPosition : partSize;
            if (position != expectedPosition || size != expectedSize) {
                throw new IOException(
                    "invalid part range for part ["
                        + partNumber
                        + "], expected position/size ["
                        + expectedPosition
                        + "/"
                        + expectedSize
                        + "] but received ["
                        + position
                        + "/"
                        + size
                        + "]"
                );
            }
            final TrackingInputStream inputStream = new TrackingInputStream(contents, Math.toIntExact(position), Math.toIntExact(size));
            final TrackedPart trackedPart = new TrackedPart(partNumber, position, size, inputStream);
            if (parts.putIfAbsent(partNumber, trackedPart) != null) {
                throw new IOException("part [" + partNumber + "] was requested more than once");
            }
            return new InputStreamContainer(inputStream, size, position);
        }

        private int providedStreamCount() {
            return parts.size();
        }

        private int numberOfParts() {
            return numberOfParts;
        }

        private void assertFullyConsumedAndClosed() {
            assertEquals(1, streamContextCalls.get());
            assertEquals(numberOfParts, parts.size());
            long expectedPosition = 0;
            for (int partNumber = 0; partNumber < numberOfParts; partNumber++) {
                final TrackedPart part = parts.get(partNumber);
                assertNotNull("missing part [" + partNumber + "]", part);
                assertEquals(partNumber, part.partNumber);
                assertEquals(expectedPosition, part.position);
                assertEquals(part.size, part.inputStream.bytesRead());
                assertEquals("part stream must close exactly once", 1, part.inputStream.closeCalls());
                expectedPosition += part.size;
            }
            assertEquals(contents.length, expectedPosition);
        }

        private void assertOpenedStreamsClosed() {
            assertTrue("stream context requested more than once", streamContextCalls.get() <= 1);
            for (TrackedPart part : parts.values()) {
                assertEquals("opened part stream must close exactly once", 1, part.inputStream.closeCalls());
            }
        }
    }

    private static final class TrackedPart {
        private final int partNumber;
        private final long position;
        private final long size;
        private final TrackingInputStream inputStream;

        private TrackedPart(int partNumber, long position, long size, TrackingInputStream inputStream) {
            this.partNumber = partNumber;
            this.position = position;
            this.size = size;
            this.inputStream = inputStream;
        }
    }

    private static final class TrackingInputStream extends InputStream {
        private final byte[] contents;
        private final int start;
        private final int length;
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicBoolean closed = new AtomicBoolean();

        private TrackingInputStream(byte[] contents, int start, int length) {
            this.contents = contents;
            this.start = start;
            this.length = length;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            final long offset = bytesRead.get();
            if (offset >= length) {
                return -1;
            }
            bytesRead.incrementAndGet();
            return Byte.toUnsignedInt(contents[start + Math.toIntExact(offset)]);
        }

        @Override
        public int read(byte[] buffer, int offset, int requestedLength) throws IOException {
            ensureOpen();
            if (offset < 0 || requestedLength < 0 || requestedLength > buffer.length - offset) {
                throw new IndexOutOfBoundsException();
            }
            if (requestedLength == 0) {
                return 0;
            }
            final long currentOffset = bytesRead.get();
            if (currentOffset >= length) {
                return -1;
            }
            final int bytesToRead = Math.min(requestedLength, Math.toIntExact(length - currentOffset));
            System.arraycopy(contents, start + Math.toIntExact(currentOffset), buffer, offset, bytesToRead);
            bytesRead.addAndGet(bytesToRead);
            return bytesToRead;
        }

        @Override
        public int available() throws IOException {
            ensureOpen();
            return Math.toIntExact(length - bytesRead.get());
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            closed.set(true);
        }

        private void ensureOpen() throws IOException {
            if (closed.get()) {
                throw new IOException("stream is closed");
            }
        }

        private long bytesRead() {
            return bytesRead.get();
        }

        private int closeCalls() {
            return closeCalls.get();
        }
    }
}
