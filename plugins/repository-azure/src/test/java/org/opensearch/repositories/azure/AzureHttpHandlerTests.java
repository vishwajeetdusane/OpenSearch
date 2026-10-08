/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.azure;

import com.sun.net.httpserver.HttpServer;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import fixture.azure.AzureHttpHandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@SuppressForbidden(reason = "uses a local HTTP server to test the Azure fixture")
public class AzureHttpHandlerTests extends OpenSearchTestCase {

    private AzureHttpHandler handler;
    private HttpServer server;
    private ExecutorService serverExecutor;
    private ExecutorService requestExecutor;
    private String endpoint;

    @Before
    public void startServer() throws Exception {
        handler = new AzureHttpHandler("container");
        server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        serverExecutor = Executors.newFixedThreadPool(4);
        requestExecutor = Executors.newFixedThreadPool(4);
        server.setExecutor(serverExecutor);
        server.createContext("/container", handler);
        server.start();
        endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @After
    public void stopServer() throws Exception {
        server.stop(0);
        shutdown(requestExecutor);
        shutdown(serverExecutor);
    }

    public void testStagesIdenticalBlockIdsPerBlobAndHonorsCommitOrder() throws Exception {
        final String firstBlock = blockId("block-0");
        final String secondBlock = blockId("block-1");

        assertSuccessful(
            runConcurrently(
                () -> putBlock("first", firstBlock, "first-0", Map.of("x-ms-meta-stage", "first")),
                () -> putBlock("second", firstBlock, "second-0", Map.of("x-ms-meta-stage", "second"))
            )
        );
        assertSuccessful(
            runConcurrently(
                () -> putBlock("first", secondBlock, "first-1", Map.of("x-ms-meta-stage", "first")),
                () -> putBlock("second", secondBlock, "second-1", Map.of("x-ms-meta-stage", "second"))
            )
        );

        assertThat(
            commitBlockList("first", blockList(latest(secondBlock), latest(firstBlock)), Map.of("x-ms-meta-owner", "first")).status,
            equalTo(201)
        );
        assertThat(
            commitBlockList("second", blockList(latest(firstBlock), latest(secondBlock)), Map.of("x-ms-meta-owner", "second")).status,
            equalTo(201)
        );

        assertBlob("first", "first-1first-0");
        assertBlob("second", "second-0second-1");
        assertThat(handler.uncommittedBlocks(), not(hasKey(blobPath("first"))));
        assertThat(handler.uncommittedBlocks(), not(hasKey(blobPath("second"))));

        final Response firstProperties = headBlob("first");
        assertThat(firstProperties.header("x-ms-meta-owner"), equalTo("first"));
        assertThat(firstProperties.header("x-ms-meta-stage"), nullValue());
        final Response secondProperties = headBlob("second");
        assertThat(secondProperties.header("x-ms-meta-owner"), equalTo("second"));
        assertThat(secondProperties.header("x-ms-meta-stage"), nullValue());
    }

    public void testPutBlobConditionsUseAzureErrorSemantics() throws Exception {
        final Response created = putBlob("conditional-blob", "original", Map.of("If-None-Match", "*", "x-ms-meta-version", "original"));
        assertThat(created.status, equalTo(201));
        final String originalETag = created.header("ETag");
        assertThat(originalETag, not(nullValue()));

        final Response alreadyExists = putBlob(
            "conditional-blob",
            "must-not-win",
            Map.of("If-None-Match", "*", "x-ms-meta-version", "conflict")
        );
        assertAzureError(alreadyExists, 409, "BlobAlreadyExists");
        assertBlob("conditional-blob", "original");
        assertThat(headBlob("conditional-blob").header("ETag"), equalTo(originalETag));

        final Response stale = putBlob("conditional-blob", "stale", Map.of("If-Match", "\"stale\"", "x-ms-meta-version", "stale"));
        assertAzureError(stale, 412, "ConditionNotMet");
        assertBlob("conditional-blob", "original");
        assertThat(headBlob("conditional-blob").header("x-ms-meta-version"), equalTo("original"));

        final Response replaced = putBlob(
            "conditional-blob",
            "replacement",
            Map.of("If-Match", originalETag, "x-ms-meta-version", "replacement")
        );
        assertThat(replaced.status, equalTo(201));
        assertThat(replaced.header("ETag"), not(equalTo(originalETag)));
        assertBlob("conditional-blob", "replacement");
        assertThat(headBlob("conditional-blob").header("x-ms-meta-version"), equalTo("replacement"));
    }

    public void testPutBlockListIfMatchPreservesFailedBlocksAndWinningBlob() throws Exception {
        final Response original = putBlob("matched-blob", "original", Map.of("x-ms-meta-owner", "original"));
        final String originalETag = original.header("ETag");
        final String staleBlock = blockId("stale-0");
        final String currentBlock = blockId("current-0");

        assertThat(putBlock("matched-blob", staleBlock, "stale", Map.of("x-ms-meta-stage", "stale")).status, equalTo(201));
        final Response staleCommit = commitBlockList(
            "matched-blob",
            blockList(latest(staleBlock)),
            Map.of("If-Match", "\"stale\"", "x-ms-meta-owner", "stale")
        );
        assertAzureError(staleCommit, 412, "ConditionNotMet");
        assertBlob("matched-blob", "original");
        assertThat(headBlob("matched-blob").header("ETag"), equalTo(originalETag));
        assertThat(headBlob("matched-blob").header("x-ms-meta-owner"), equalTo("original"));
        assertThat(handler.uncommittedBlocks().get(blobPath("matched-blob")).keySet(), equalTo(Set.of(staleBlock)));
        assertThat(uncommittedBlockContents("matched-blob", staleBlock), equalTo("stale"));

        assertThat(putBlock("matched-blob", currentBlock, "current", Map.of("x-ms-meta-stage", "current")).status, equalTo(201));
        final Response currentCommit = commitBlockList(
            "matched-blob",
            blockList(uncommitted(currentBlock)),
            Map.of("If-Match", originalETag, "x-ms-meta-owner", "current")
        );
        assertThat(currentCommit.status, equalTo(201));
        assertBlob("matched-blob", "current");
        assertThat(headBlob("matched-blob").header("x-ms-meta-owner"), equalTo("current"));
        assertThat(headBlob("matched-blob").header("x-ms-meta-stage"), nullValue());
        assertThat(handler.uncommittedBlocks().get(blobPath("matched-blob")).keySet(), equalTo(Set.of(staleBlock)));
        assertThat(uncommittedBlockContents("matched-blob", staleBlock), equalTo("stale"));

        final String tailBlock = blockId("tail-0");
        assertThat(putBlock("matched-blob", tailBlock, "tail", Map.of()).status, equalTo(201));
        final Response reorderedCommit = commitBlockList(
            "matched-blob",
            blockList(uncommitted(tailBlock), committed(currentBlock)),
            Map.of("If-Match", currentCommit.header("ETag"), "x-ms-meta-owner", "reordered")
        );
        assertThat(reorderedCommit.status, equalTo(201));
        assertBlob("matched-blob", "tailcurrent");
        assertThat(handler.uncommittedBlocks().get(blobPath("matched-blob")).keySet(), equalTo(Set.of(staleBlock)));
        assertThat(uncommittedBlockContents("matched-blob", staleBlock), equalTo("stale"));
    }

    public void testExactlyOneConcurrentCreateOnlyBlockListWins() throws Exception {
        final String firstBlock = blockId("attempt-one-0");
        final String secondBlock = blockId("attempt-two-0");
        assertThat(putBlock("create-only", firstBlock, "one", Map.of()).status, equalTo(201));
        assertThat(putBlock("create-only", secondBlock, "two", Map.of()).status, equalTo(201));

        final List<Response> responses = runConcurrently(
            () -> commitBlockList("create-only", blockList(latest(firstBlock)), Map.of("If-None-Match", "*", "x-ms-meta-attempt", "one")),
            () -> commitBlockList("create-only", blockList(latest(secondBlock)), Map.of("If-None-Match", "*", "x-ms-meta-attempt", "two"))
        );
        assertThat(responses.stream().map(response -> response.status).sorted().toList(), equalTo(List.of(201, 412)));
        final Response failed = responses.stream().filter(response -> response.status == 412).findFirst().orElseThrow();
        assertAzureError(failed, 412, "ConditionNotMet");

        final String winningContents = blobContents("create-only");
        final String winningBlock = "one".equals(winningContents) ? firstBlock : secondBlock;
        final String abandonedBlock = firstBlock.equals(winningBlock) ? secondBlock : firstBlock;
        final String abandonedContents = firstBlock.equals(abandonedBlock) ? "one" : "two";
        assertThat(headBlob("create-only").header("x-ms-meta-attempt"), equalTo(winningContents));
        assertThat(handler.uncommittedBlocks().get(blobPath("create-only")).keySet(), equalTo(Set.of(abandonedBlock)));
        assertThat(uncommittedBlockContents("create-only", abandonedBlock), equalTo(abandonedContents));
        assertThat(handler.uncommittedBlocks().get(blobPath("create-only")).get(winningBlock), nullValue());
    }

    private Response putBlock(final String blobName, final String blockId, final String contents, final Map<String, String> headers)
        throws IOException {
        final String encodedBlockId = URLEncoder.encode(blockId, UTF_8);
        return send("PUT", blobName + "?comp=block&blockid=" + encodedBlockId, contents.getBytes(UTF_8), headers);
    }

    private Response commitBlockList(final String blobName, final String blockList, final Map<String, String> headers) throws IOException {
        return send("PUT", blobName + "?comp=blocklist", blockList.getBytes(UTF_8), headers);
    }

    private Response putBlob(final String blobName, final String contents, final Map<String, String> headers) throws IOException {
        return send("PUT", blobName, contents.getBytes(UTF_8), headers);
    }

    private Response headBlob(final String blobName) throws IOException {
        return send("HEAD", blobName, null, Map.of());
    }

    private Response send(final String method, final String resource, final byte[] body, final Map<String, String> headers)
        throws IOException {
        final HttpURLConnection connection = (HttpURLConnection) URI.create(endpoint + "/container/" + resource).toURL().openConnection();
        try {
            connection.setRequestMethod(method);
            headers.forEach(connection::setRequestProperty);
            if (body != null) {
                connection.setDoOutput(true);
                connection.setFixedLengthStreamingMode(body.length);
                connection.getOutputStream().write(body);
            }

            final int status = connection.getResponseCode();
            final Map<String, List<String>> responseHeaders = new HashMap<>();
            connection.getHeaderFields().forEach((name, values) -> {
                if (name != null) {
                    responseHeaders.put(name, List.copyOf(values));
                }
            });
            final InputStream responseStream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
            final String responseBody = responseStream == null ? "" : new String(responseStream.readAllBytes(), UTF_8);
            if (responseStream != null) {
                responseStream.close();
            }
            return new Response(status, responseHeaders, responseBody);
        } finally {
            connection.disconnect();
        }
    }

    @SafeVarargs
    private final List<Response> runConcurrently(final Callable<Response>... requests) throws Exception {
        final CountDownLatch ready = new CountDownLatch(requests.length);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<Response>> futures = new ArrayList<>();
        for (Callable<Response> request : requests) {
            futures.add(requestExecutor.submit(() -> {
                ready.countDown();
                start.await();
                return request.call();
            }));
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        start.countDown();

        final List<Response> responses = new ArrayList<>();
        for (Future<Response> future : futures) {
            responses.add(future.get(10, TimeUnit.SECONDS));
        }
        return responses;
    }

    private void assertSuccessful(final List<Response> responses) {
        assertThat(responses.stream().map(response -> response.status).toList(), equalTo(List.of(201, 201)));
    }

    private void assertAzureError(final Response response, final int status, final String errorCode) {
        assertThat(response.status, equalTo(status));
        assertThat(response.header("x-ms-error-code"), equalTo(errorCode));
        assertThat(response.body, containsString("<Code>" + errorCode + "</Code>"));
    }

    private void assertBlob(final String blobName, final String expectedContents) {
        assertThat(blobContents(blobName), equalTo(expectedContents));
    }

    private String blobContents(final String blobName) {
        final BytesReference contents = handler.blobs().get(blobPath(blobName));
        assertNotNull(contents);
        return new String(BytesReference.toBytes(contents), UTF_8);
    }

    private String uncommittedBlockContents(final String blobName, final String blockId) {
        final BytesReference contents = handler.uncommittedBlocks().get(blobPath(blobName)).get(blockId);
        assertNotNull(contents);
        return new String(BytesReference.toBytes(contents), UTF_8);
    }

    private static String blockList(final String... entries) {
        return "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>" + String.join("", entries) + "</BlockList>";
    }

    private static String latest(final String blockId) {
        return "<Latest>" + blockId + "</Latest>";
    }

    private static String committed(final String blockId) {
        return "<Committed>" + blockId + "</Committed>";
    }

    private static String uncommitted(final String blockId) {
        return "<Uncommitted>" + blockId + "</Uncommitted>";
    }

    private static String blockId(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String blobPath(final String blobName) {
        return "/container/" + blobName;
    }

    private static void shutdown(final ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    private static class Response {
        private final int status;
        private final Map<String, List<String>> headers;
        private final String body;

        private Response(final int status, final Map<String, List<String>> headers, final String body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        private String header(final String name) {
            return headers.entrySet()
                .stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .map(entry -> entry.getValue().get(0))
                .findFirst()
                .orElse(null);
        }
    }
}
