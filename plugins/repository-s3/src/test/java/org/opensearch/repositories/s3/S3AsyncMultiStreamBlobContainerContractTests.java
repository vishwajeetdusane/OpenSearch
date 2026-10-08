/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.blobstore.AbstractAsyncMultiStreamBlobContainerTestCase;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.TransferSemaphoresHolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.invocation.InvocationOnMock;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3AsyncMultiStreamBlobContainerContractTests extends AbstractAsyncMultiStreamBlobContainerTestCase
    implements
        ConfigPathSupport {

    @Override
    protected AsyncUploadTestHarness createTestHarness() throws Exception {
        return new S3AsyncUploadTestHarness(configPath());
    }

    @Override
    protected Set<Deviation> deviations() {
        return Set.of(Deviation.FAIL_IF_ALREADY_EXISTS_IGNORED, Deviation.SINGLE_PART_FINALIZER_FAILURE_DELETES_EXISTING_OBJECT);
    }

    @Override
    protected WritePriority writePriority() {
        // S3 HIGH uploads use the async transfer manager directly; admission and queue behavior remain S3-specific tests.
        return WritePriority.HIGH;
    }

    private static final class S3AsyncUploadTestHarness implements AsyncUploadTestHarness {
        private static final long PART_SIZE = ByteSizeUnit.MB.toBytes(5);
        private static final long MULTIPART_UPLOAD_SIZE = (2 * PART_SIZE) + 257;
        private static final long SINGLE_UPLOAD_SIZE = 1024;
        private static final long AWAIT_TIMEOUT_SECONDS = 30;

        private final StatefulAsyncClient statefulClient;
        private final StatefulS3AsyncService asyncService;
        private final ExecutorService streamReaderExecutor;
        private final S3BlobContainer container;

        private S3AsyncUploadTestHarness(Path configPath) {
            final S3AsyncClient asyncClient = mock(S3AsyncClient.class);
            statefulClient = new StatefulAsyncClient(asyncClient);
            statefulClient.configure();
            asyncService = new StatefulS3AsyncService(configPath, asyncClient);
            streamReaderExecutor = Executors.newFixedThreadPool(4);
            container = new S3BlobContainer(BlobPath.cleanPath(), createBlobStore());
        }

        @Override
        public AsyncMultiStreamBlobContainer container() {
            return container;
        }

        @Override
        public long multipartUploadSize() {
            return MULTIPART_UPLOAD_SIZE;
        }

        @Override
        public long singleUploadSize() {
            return SINGLE_UPLOAD_SIZE;
        }

        @Override
        public StoredBlob getBlob(String blobName) {
            return statefulClient.getBlob(blobName);
        }

        @Override
        public void putBlob(String blobName, byte[] contents, Map<String, String> metadata) {
            statefulClient.putBlob(blobName, contents, metadata);
        }

        @Override
        public void setExpectedPartCount(int partCount) {
            statefulClient.setExpectedPartCount(partCount);
        }

        @Override
        public void setPartCompletionOrder(PartCompletionOrder completionOrder) {
            statefulClient.setPartCompletionOrder(completionOrder);
        }

        @Override
        public List<Integer> completedPartOrder() {
            return statefulClient.completedPartOrder();
        }

        @Override
        public void failNextUpload(FailureStage failureStage) {
            statefulClient.failNextUpload(failureStage);
        }

        @Override
        public void holdFinalPublication() {
            statefulClient.holdFinalPublication();
        }

        @Override
        public void awaitFinalPublicationAttempt() throws Exception {
            statefulClient.awaitFinalPublicationAttempt();
        }

        @Override
        public void releaseFinalPublication() {
            statefulClient.releaseFinalPublication();
        }

        @Override
        public void assertNoDanglingUploadState() {
            statefulClient.assertNoDanglingUploadState();
        }

        @Override
        public void awaitIdle() throws Exception {
            statefulClient.awaitIdle();
        }

        @Override
        public void close() throws IOException {
            statefulClient.close();
            asyncService.close();
            streamReaderExecutor.shutdownNow();
            try {
                if (streamReaderExecutor.awaitTermination(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                    throw new IOException("timed out stopping the S3 contract stream reader");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping the S3 contract stream reader", e);
            }
        }

        private S3BlobStore createBlobStore() {
            final Settings settings = Settings.builder()
                .put(S3Repository.CLIENT_NAME.getKey(), "async-contract")
                .put(S3Repository.UPLOAD_RETRY_ENABLED.getKey(), false)
                .put(S3Repository.PERMIT_BACKED_TRANSFER_ENABLED.getKey(), false)
                .build();
            final RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repository", S3Repository.TYPE, settings);
            final GenericStatsMetricPublisher genericStatsMetricPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
            final AsyncExecutorContainer unusedExecutorContainer = mock(AsyncExecutorContainer.class);
            final SizeBasedBlockingQ unusedQueue = mock(SizeBasedBlockingQ.class);
            when(unusedQueue.isMaxCapacityBelowContentLength(anyLong())).thenReturn(true);

            return new S3BlobStore(
                null,
                asyncService,
                true,
                "bucket",
                S3Repository.BUFFER_SIZE_SETTING.getDefault(Settings.EMPTY),
                S3Repository.CANNED_ACL_SETTING.getDefault(Settings.EMPTY),
                S3Repository.STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
                S3Repository.BULK_DELETE_SIZE.get(Settings.EMPTY),
                repositoryMetadata,
                new AsyncTransferManager(
                    PART_SIZE,
                    streamReaderExecutor,
                    streamReaderExecutor,
                    streamReaderExecutor,
                    new TransferSemaphoresHolder(3, 10, 5, TimeUnit.MINUTES, genericStatsMetricPublisher)
                ),
                unusedExecutorContainer,
                unusedExecutorContainer,
                unusedExecutorContainer,
                unusedQueue,
                unusedQueue,
                genericStatsMetricPublisher,
                S3Repository.SERVER_SIDE_ENCRYPTION_TYPE_SETTING.getDefault(Settings.EMPTY),
                S3Repository.SERVER_SIDE_ENCRYPTION_KMS_KEY_SETTING.getDefault(Settings.EMPTY),
                S3Repository.SERVER_SIDE_ENCRYPTION_BUCKET_KEY_SETTING.getDefault(Settings.EMPTY),
                S3Repository.SERVER_SIDE_ENCRYPTION_ENCRYPTION_CONTEXT_SETTING.getDefault(Settings.EMPTY),
                S3Repository.EXPECTED_BUCKET_OWNER_SETTING.getDefault(Settings.EMPTY)
            );
        }
    }

    private static final class StatefulS3AsyncService extends S3AsyncService {
        private final S3AsyncClient asyncClient;

        private StatefulS3AsyncService(Path configPath, S3AsyncClient asyncClient) {
            super(configPath);
            this.asyncClient = asyncClient;
        }

        @Override
        public AmazonAsyncS3Reference client(
            RepositoryMetadata repositoryMetadata,
            AsyncExecutorContainer urgentExecutorBuilder,
            AsyncExecutorContainer priorityExecutorBuilder,
            AsyncExecutorContainer normalExecutorBuilder
        ) {
            return new AmazonAsyncS3Reference(AmazonAsyncS3WithCredentials.create(asyncClient, asyncClient, asyncClient, null));
        }
    }

    private static final class StatefulAsyncClient {
        private static final long AWAIT_TIMEOUT_SECONDS = 30;

        private final S3AsyncClient client;
        private final Object partCompletionMutex = new Object();
        private final Map<String, StoredBlob> blobs = new java.util.concurrent.ConcurrentHashMap<>();
        private final Map<String, MultipartSession> multipartSessions = new java.util.concurrent.ConcurrentHashMap<>();
        private final Map<Integer, PendingPart> pendingParts = new HashMap<>();
        private final CopyOnWriteArrayList<CompletableFuture<?>> operations = new CopyOnWriteArrayList<>();
        private final List<Integer> completedPartOrder = new CopyOnWriteArrayList<>();
        private final AtomicInteger uploadIdGenerator = new AtomicInteger();
        private final AtomicInteger expectedPartCount = new AtomicInteger();
        private final AtomicReference<FailureStage> failureStage = new AtomicReference<>();
        private final AtomicBoolean failureInjected = new AtomicBoolean();
        private final AtomicBoolean holdFinalPublication = new AtomicBoolean();
        private final AtomicReference<Runnable> pendingFinalPublication = new AtomicReference<>();
        private final CountDownLatch finalPublicationAttempt = new CountDownLatch(1);

        private volatile PartCompletionOrder partCompletionOrder = PartCompletionOrder.NATURAL;

        private StatefulAsyncClient(S3AsyncClient client) {
            this.client = client;
        }

        private void configure() {
            doAnswer(this::putObject).when(client).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
            doAnswer(this::createMultipartUpload).when(client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            doAnswer(this::uploadPart).when(client).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
            doAnswer(this::completeMultipartUpload).when(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            doAnswer(this::abortMultipartUpload).when(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
            doAnswer(this::deleteObject).when(client).deleteObject(any(DeleteObjectRequest.class));
            doNothing().when(client).close();
        }

        private CompletableFuture<PutObjectResponse> putObject(InvocationOnMock invocation) {
            final PutObjectRequest request = invocation.getArgument(0);
            final AsyncRequestBody requestBody = invocation.getArgument(1);
            final CompletableFuture<PutObjectResponse> response = track(new CompletableFuture<>());
            collectBody(requestBody, request.contentLength()).whenComplete((contents, throwable) -> {
                if (throwable != null) {
                    response.completeExceptionally(throwable);
                    return;
                }
                blobs.put(request.key(), new StoredBlob(contents, request.metadata()));
                response.complete(PutObjectResponse.builder().eTag("single-etag").build());
            });
            return response;
        }

        private CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(InvocationOnMock invocation) {
            final CreateMultipartUploadRequest request = invocation.getArgument(0);
            final String uploadId = "upload-" + uploadIdGenerator.incrementAndGet();
            multipartSessions.put(uploadId, new MultipartSession(request.key(), request.metadata()));
            return track(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder().uploadId(uploadId).build()));
        }

        private CompletableFuture<UploadPartResponse> uploadPart(InvocationOnMock invocation) {
            final UploadPartRequest request = invocation.getArgument(0);
            final AsyncRequestBody requestBody = invocation.getArgument(1);
            final CompletableFuture<UploadPartResponse> response = track(new CompletableFuture<>());
            final MultipartSession session = multipartSessions.get(request.uploadId());
            if (session == null || session.key.equals(request.key()) == false) {
                response.completeExceptionally(new IOException("unknown multipart upload [" + request.uploadId() + "]"));
                return response;
            }

            collectBody(requestBody, request.contentLength()).whenComplete((contents, throwable) -> {
                if (throwable != null) {
                    response.completeExceptionally(throwable);
                    return;
                }
                session.parts.put(request.partNumber(), contents);
                final PendingPart pendingPart = new PendingPart(request.partNumber(), response);
                if (partCompletionOrder == PartCompletionOrder.REVERSE) {
                    completePartsInReverseOrderWhenReady(pendingPart);
                } else {
                    completePart(pendingPart);
                }
            });
            return response;
        }

        private CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(InvocationOnMock invocation) {
            final CompleteMultipartUploadRequest request = invocation.getArgument(0);
            final CompletableFuture<CompleteMultipartUploadResponse> response = track(new CompletableFuture<>());
            finalPublicationAttempt.countDown();

            final Runnable publication = () -> {
                if (failureStage.get() == FailureStage.FINAL_PUBLICATION && failureInjected.compareAndSet(false, true)) {
                    response.completeExceptionally(new IOException("simulated final publication failure"));
                    return;
                }
                final MultipartSession session = multipartSessions.get(request.uploadId());
                if (session == null || session.key.equals(request.key()) == false) {
                    response.completeExceptionally(new IOException("unknown multipart upload [" + request.uploadId() + "]"));
                    return;
                }
                try {
                    final ByteArrayOutputStream contents = new ByteArrayOutputStream();
                    for (CompletedPart completedPart : request.multipartUpload().parts()) {
                        final byte[] part = session.parts.get(completedPart.partNumber());
                        if (part == null) {
                            throw new IOException("missing uploaded part [" + completedPart.partNumber() + "]");
                        }
                        contents.writeBytes(part);
                    }
                    blobs.put(request.key(), new StoredBlob(contents.toByteArray(), session.metadata));
                    multipartSessions.remove(request.uploadId());
                    response.complete(CompleteMultipartUploadResponse.builder().eTag("multipart-etag").build());
                } catch (Exception e) {
                    response.completeExceptionally(e);
                }
            };

            if (holdFinalPublication.get()) {
                if (pendingFinalPublication.compareAndSet(null, publication) == false) {
                    response.completeExceptionally(new IOException("another final publication is already held"));
                } else if (holdFinalPublication.get() == false && pendingFinalPublication.compareAndSet(publication, null)) {
                    publication.run();
                }
            } else {
                publication.run();
            }
            return response;
        }

        private CompletableFuture<AbortMultipartUploadResponse> abortMultipartUpload(InvocationOnMock invocation) {
            final AbortMultipartUploadRequest request = invocation.getArgument(0);
            multipartSessions.remove(request.uploadId());
            return track(CompletableFuture.completedFuture(AbortMultipartUploadResponse.builder().build()));
        }

        private CompletableFuture<DeleteObjectResponse> deleteObject(InvocationOnMock invocation) {
            final DeleteObjectRequest request = invocation.getArgument(0);
            blobs.remove(request.key());
            return track(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
        }

        private CompletableFuture<byte[]> collectBody(AsyncRequestBody requestBody, long expectedLength) {
            final CompletableFuture<byte[]> collected = track(new CompletableFuture<>());
            requestBody.subscribe(new Subscriber<>() {
                private final ByteArrayOutputStream contents = new ByteArrayOutputStream();
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    try {
                        final ByteBuffer copy = item.asReadOnlyBuffer();
                        final byte[] bytes = new byte[copy.remaining()];
                        copy.get(bytes);
                        contents.writeBytes(bytes);
                    } catch (Exception e) {
                        subscription.cancel();
                        collected.completeExceptionally(e);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    collected.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    if (contents.size() != expectedLength) {
                        collected.completeExceptionally(
                            new IOException(
                                "request body length [" + contents.size() + "] did not match declared length [" + expectedLength + "]"
                            )
                        );
                    } else {
                        collected.complete(contents.toByteArray());
                    }
                }
            });
            return collected;
        }

        private void completePartsInReverseOrderWhenReady(PendingPart pendingPart) {
            List<PendingPart> readyParts = null;
            synchronized (partCompletionMutex) {
                pendingParts.put(pendingPart.partNumber, pendingPart);
                if (pendingParts.size() == expectedPartCount.get()) {
                    readyParts = new ArrayList<>(pendingParts.values());
                    readyParts.sort(Comparator.comparingInt((PendingPart part) -> part.partNumber).reversed());
                    pendingParts.clear();
                }
            }
            if (readyParts != null) {
                readyParts.forEach(this::completePart);
            }
        }

        private void completePart(PendingPart pendingPart) {
            final int failedPartNumber = Math.min(2, expectedPartCount.get());
            if (failureStage.get() == FailureStage.PART_UPLOAD
                && pendingPart.partNumber == failedPartNumber
                && failureInjected.compareAndSet(false, true)) {
                pendingPart.response.completeExceptionally(new IOException("simulated part upload failure"));
                return;
            }
            completedPartOrder.add(pendingPart.partNumber - 1);
            pendingPart.response.complete(UploadPartResponse.builder().eTag("part-etag-" + pendingPart.partNumber).build());
        }

        private void setExpectedPartCount(int partCount) {
            if (partCount <= 0 || expectedPartCount.compareAndSet(0, partCount) == false) {
                throw new IllegalStateException("expected part count already set or invalid");
            }
        }

        private void setPartCompletionOrder(PartCompletionOrder completionOrder) {
            this.partCompletionOrder = completionOrder;
        }

        private List<Integer> completedPartOrder() {
            return List.copyOf(completedPartOrder);
        }

        private void failNextUpload(FailureStage failureStage) {
            if (this.failureStage.compareAndSet(null, failureStage) == false) {
                throw new IllegalStateException("failure stage already configured");
            }
        }

        private void holdFinalPublication() {
            holdFinalPublication.set(true);
        }

        private void awaitFinalPublicationAttempt() throws InterruptedException {
            if (finalPublicationAttempt.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                throw new AssertionError("timed out waiting for final publication attempt");
            }
        }

        private void releaseFinalPublication() {
            holdFinalPublication.set(false);
            final Runnable publication = pendingFinalPublication.getAndSet(null);
            if (publication != null) {
                publication.run();
            }
        }

        private void assertNoDanglingUploadState() {
            if (multipartSessions.isEmpty() == false) {
                throw new AssertionError("dangling S3 multipart uploads " + multipartSessions.keySet());
            }
        }

        private StoredBlob getBlob(String blobName) {
            return blobs.get(blobName);
        }

        private void putBlob(String blobName, byte[] contents, Map<String, String> metadata) {
            blobs.put(blobName, new StoredBlob(contents, metadata));
        }

        private void awaitIdle() throws InterruptedException, TimeoutException {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_TIMEOUT_SECONDS);
            while (true) {
                final List<CompletableFuture<?>> snapshot = List.copyOf(operations);
                final CompletableFuture<?>[] settled = snapshot.stream()
                    .map(future -> future.handle((response, throwable) -> null))
                    .toArray(CompletableFuture[]::new);
                final long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new TimeoutException("timed out waiting for S3 contract operations");
                }
                try {
                    CompletableFuture.allOf(settled).get(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (ExecutionException e) {
                    throw new AssertionError("settled operation unexpectedly failed", e);
                }
                if (snapshot.size() == operations.size() && operations.stream().allMatch(CompletableFuture::isDone)) {
                    return;
                }
            }
        }

        private <T> CompletableFuture<T> track(CompletableFuture<T> future) {
            operations.add(future);
            return future;
        }

        private void close() {
            releaseFinalPublication();
            final List<PendingPart> unfinishedParts;
            synchronized (partCompletionMutex) {
                unfinishedParts = new ArrayList<>(pendingParts.values());
                pendingParts.clear();
            }
            unfinishedParts.forEach(part -> part.response.completeExceptionally(new IOException("test harness closed")));
            operations.forEach(future -> future.completeExceptionally(new IOException("test harness closed")));
        }
    }

    private static final class MultipartSession {
        private final String key;
        private final Map<String, String> metadata;
        private final Map<Integer, byte[]> parts = new java.util.concurrent.ConcurrentHashMap<>();

        private MultipartSession(String key, Map<String, String> metadata) {
            this.key = key;
            this.metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }
    }

    private static final class PendingPart {
        private final int partNumber;
        private final CompletableFuture<UploadPartResponse> response;

        private PendingPart(int partNumber, CompletableFuture<UploadPartResponse> response) {
            this.partNumber = partNumber;
            this.response = response;
        }
    }
}
