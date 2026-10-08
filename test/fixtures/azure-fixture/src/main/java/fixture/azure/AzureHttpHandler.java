/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package fixture.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.Streams;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Minimal HTTP handler that acts as an Azure compliant server
 */
@SuppressForbidden(reason = "Uses a HttpServer to emulate an Azure endpoint")
public class AzureHttpHandler implements HttpHandler {

    private final Object stateLock = new Object();
    private final Map<String, Blob> blobs;
    private final Map<String, Map<String, BytesReference>> uncommittedBlocks;
    private final String container;
    private long nextETag;

    public AzureHttpHandler(final String container) {
        this.container = Objects.requireNonNull(container);
        this.blobs = new HashMap<>();
        this.uncommittedBlocks = new HashMap<>();
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
        if (request.startsWith("GET") || request.startsWith("HEAD") || request.startsWith("DELETE")) {
            int read = exchange.getRequestBody().read();
            assert read == -1 : "Request body should have been empty but saw [" + read + "]";
        }
        try {
            if (Regex.simpleMatch("PUT /" + container + "/*blockid=*", request)) {
                // Put Block (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block)
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                final String blockId = params.get("blockid");
                final BytesReference block = Streams.readFully(exchange.getRequestBody());
                synchronized (stateLock) {
                    uncommittedBlocks.computeIfAbsent(exchange.getRequestURI().getPath(), ignored -> new HashMap<>()).put(blockId, block);
                }
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("PUT /" + container + "/*comp=blocklist*", request)) {
                // Put Block List (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list)
                final List<BlockReference> blockReferences;
                try {
                    blockReferences = parseBlockList(exchange);
                } catch (XMLStreamException e) {
                    sendError(exchange, RestStatus.BAD_REQUEST, "InvalidBlockList");
                    return;
                }

                final String blobPath = exchange.getRequestURI().getPath();
                StorageError error;
                Blob committedBlob = null;
                synchronized (stateLock) {
                    final Blob currentBlob = blobs.get(blobPath);
                    error = conditionError(exchange.getRequestHeaders(), currentBlob);
                    if (error == null) {
                        try {
                            committedBlob = commitBlockList(blobPath, currentBlob, blockReferences, readMetadata(exchange.getRequestHeaders()));
                            blobs.put(blobPath, committedBlob);
                        } catch (InvalidBlockListException e) {
                            error = new StorageError(RestStatus.BAD_REQUEST, "InvalidBlockList");
                        }
                    }
                }
                if (error != null) {
                    sendError(exchange, error.status, error.code);
                    return;
                }
                addCommittedBlobHeaders(exchange.getResponseHeaders(), committedBlob);
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("PUT /" + container + "/*", request)) {
                // PUT Blob (see https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob)
                final BytesReference contents = Streams.readFully(exchange.getRequestBody());
                StorageError error;
                Blob committedBlob = null;
                synchronized (stateLock) {
                    final String blobPath = exchange.getRequestURI().getPath();
                    error = conditionError(exchange.getRequestHeaders(), blobs.get(blobPath));
                    if (error == null) {
                        committedBlob = new Blob(contents, readMetadata(exchange.getRequestHeaders()), nextETag(), Collections.emptyMap());
                        blobs.put(blobPath, committedBlob);
                    }
                }
                if (error != null) {
                    sendError(exchange, error.status, error.code);
                    return;
                }
                addCommittedBlobHeaders(exchange.getResponseHeaders(), committedBlob);
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

            } else if (Regex.simpleMatch("HEAD /" + container + "/*", request)) {
                // Get Blob Properties (see https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties)
                final Blob blob;
                synchronized (stateLock) {
                    blob = blobs.get(exchange.getRequestURI().getPath());
                }
                if (blob == null) {
                    sendError(exchange, RestStatus.NOT_FOUND);
                    return;
                }
                exchange.getResponseHeaders().add("Content-Length", String.valueOf(blob.contents.length()));
                exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                addCommittedBlobHeaders(exchange.getResponseHeaders(), blob);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

            } else if (Regex.simpleMatch("GET /" + container + "/*", request)) {
                // GET Object (https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html)
                final Blob blob;
                synchronized (stateLock) {
                    blob = blobs.get(exchange.getRequestURI().getPath());
                }
                if (blob == null) {
                    sendError(exchange, RestStatus.NOT_FOUND);
                    return;
                }

                // see Constants.HeaderConstants.STORAGE_RANGE_HEADER
                final String range = exchange.getRequestHeaders().getFirst("x-ms-range");
                final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$").matcher(range);
                if (matcher.matches() == false) {
                    throw new AssertionError("Range header does not match expected format: " + range);
                }

                final int start = Integer.parseInt(matcher.group(1));
                final int end = Integer.parseInt(matcher.group(2));
                final int length = Math.min(end - start + 1, blob.contents.length());

                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                addCommittedBlobHeaders(exchange.getResponseHeaders(), blob);
                exchange.getResponseHeaders()
                    .add("Content-Range", "bytes " + start + "-" + Math.min(end, length) + "/" + blob.contents.length());

                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                exchange.getResponseBody().write(blob.contents.toBytesRef().bytes, start, length);

            } else if (Regex.simpleMatch("DELETE /" + container + "/*", request)) {
                // Delete Blob (https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob)
                synchronized (stateLock) {
                    blobs.entrySet().removeIf(blob -> blob.getKey().startsWith(exchange.getRequestURI().getPath()));
                }
                exchange.sendResponseHeaders(RestStatus.ACCEPTED.getStatus(), -1);

            } else if (Regex.simpleMatch("GET /container?restype=container&comp=list*", request)) {
                // List Blobs (https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs)
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                final StringBuilder list = new StringBuilder();
                list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                list.append("<EnumerationResults>");
                final String prefix = params.get("prefix");
                final Set<String> blobPrefixes = new HashSet<>();
                // Always use the delimiter (explicit or implicit), some APIs do not send it anymore
                final String delimiter = params.get("delimiter");
                if (delimiter != null) {
                    list.append("<Delimiter>").append(delimiter).append("</Delimiter>");
                }
                list.append("<Blobs>");
                final Map<String, Blob> blobsSnapshot;
                synchronized (stateLock) {
                    blobsSnapshot = new HashMap<>(blobs);
                }
                for (Map.Entry<String, Blob> blob : blobsSnapshot.entrySet()) {
                    if (prefix != null && blob.getKey().startsWith("/" + container + "/" + prefix) == false) {
                        continue;
                    }
                    String blobPath = blob.getKey().replace("/" + container + "/", "");
                    if (delimiter != null) {
                        int fromIndex = (prefix != null ? prefix.length() : 0);
                        int delimiterPosition = blobPath.indexOf(delimiter, fromIndex);
                        if (delimiterPosition > 0) {
                            blobPrefixes.add(blobPath.substring(0, delimiterPosition) + delimiter);
                            continue;
                        }
                    }

                    list.append("<Blob>");
                    if (delimiter != null) {
                        list.append("<IsPrefix>").append(blobPath.endsWith(delimiter)).append("</IsPrefix>");
                    }
                    list.append("<Name>").append(blobPath).append("</Name>");
                    list.append("<Properties><Content-Length>").append(blob.getValue().contents.length()).append("</Content-Length>");
                    list.append("<BlobType>BlockBlob</BlobType></Properties>");
                    list.append("</Blob>");
                }
                if (blobPrefixes.isEmpty() == false) {
                    blobPrefixes.forEach(p -> list.append("<BlobPrefix><Name>").append(p).append("</Name></BlobPrefix>"));

                }
                list.append("</Blobs>");
                list.append("<NextMarker />");
                list.append("</EnumerationResults>");

                byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);

            } else {
                sendError(exchange, RestStatus.BAD_REQUEST);
            }
        } finally {
            exchange.close();
        }
    }

    public Map<String, BytesReference> blobs() {
        synchronized (stateLock) {
            final Map<String, BytesReference> contents = new HashMap<>();
            blobs.forEach((path, blob) -> contents.put(path, blob.contents));
            return Collections.unmodifiableMap(contents);
        }
    }

    /**
     * Returns a snapshot of blocks that have been staged but not committed, keyed by blob path and block ID.
     */
    public Map<String, Map<String, BytesReference>> uncommittedBlocks() {
        synchronized (stateLock) {
            final Map<String, Map<String, BytesReference>> blocks = new HashMap<>();
            uncommittedBlocks.forEach((path, blobBlocks) -> blocks.put(path, Collections.unmodifiableMap(new HashMap<>(blobBlocks))));
            return Collections.unmodifiableMap(blocks);
        }
    }

    public static void sendError(final HttpExchange exchange, final RestStatus status) throws IOException {
        sendError(exchange, status, toAzureErrorCode(status));
    }

    private static void sendError(final HttpExchange exchange, final RestStatus status, final String errorCode) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");

        // see Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER
        final String requestId = exchange.getRequestHeaders().getFirst("x-ms-client-request-id");
        if (requestId != null) {
            // see Constants.HeaderConstants.STORAGE_RANGE_HEADER
            headers.add("x-ms-request-id", requestId);
        }

        // see Constants.HeaderConstants.ERROR_CODE
        headers.add("x-ms-error-code", errorCode);

        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(status.getStatus(), -1L);
        } else {
            final byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + errorCode + "</Code><Message>"
                + status + "</Message></Error>").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        }
    }

    // See https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
    private static String toAzureErrorCode(final RestStatus status) {
        assert status.getStatus() >= 400;
        switch (status) {
            case BAD_REQUEST:
                return "InvalidMetadata";
            case NOT_FOUND:
                return "BlobNotFound";
            case INTERNAL_SERVER_ERROR:
                return "InternalError";
            case SERVICE_UNAVAILABLE:
                return "ServerBusy";
            case CONFLICT:
                return "BlobAlreadyExists";
            case PRECONDITION_FAILED:
                return "ConditionNotMet";
            default:
                throw new IllegalArgumentException("Error code [" + status.getStatus() + "] is not mapped to an existing Azure code");
        }
    }

    private List<BlockReference> parseBlockList(final HttpExchange exchange) throws XMLStreamException {
        final XMLInputFactory inputFactory = XMLInputFactory.newFactory();
        inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

        final List<BlockReference> blockReferences = new java.util.ArrayList<>();
        final XMLStreamReader reader = inputFactory.createXMLStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8.name());
        boolean sawBlockList = false;
        try {
            while (reader.hasNext()) {
                if (reader.next() != XMLStreamConstants.START_ELEMENT) {
                    continue;
                }
                final String element = reader.getLocalName();
                if ("BlockList".equals(element)) {
                    if (sawBlockList) {
                        throw new XMLStreamException("Multiple BlockList elements");
                    }
                    sawBlockList = true;
                    continue;
                }
                final BlockType type = BlockType.fromElement(element);
                if (sawBlockList == false || type == null) {
                    throw new XMLStreamException("Unexpected element [" + element + "]");
                }
                final String blockId = reader.getElementText().trim();
                if (blockId.isEmpty()) {
                    throw new XMLStreamException("Block ID must not be empty");
                }
                blockReferences.add(new BlockReference(type, blockId));
            }
        } finally {
            reader.close();
        }
        if (sawBlockList == false) {
            throw new XMLStreamException("Missing BlockList element");
        }
        return blockReferences;
    }

    private Blob commitBlockList(
        final String blobPath,
        final Blob currentBlob,
        final List<BlockReference> blockReferences,
        final Map<String, String> metadata
    ) throws InvalidBlockListException {
        final Map<String, BytesReference> blobUncommittedBlocks = uncommittedBlocks.getOrDefault(blobPath, Collections.emptyMap());
        final Map<String, BytesReference> committedBlocks = currentBlob == null
            ? Collections.emptyMap()
            : currentBlob.committedBlocks;
        final Map<String, BytesReference> newCommittedBlocks = new HashMap<>();
        final Set<String> consumedUncommittedBlockIds = new HashSet<>();
        final ByteArrayOutputStream contents = new ByteArrayOutputStream();

        for (BlockReference blockReference : blockReferences) {
            BytesReference block = null;
            if (blockReference.type != BlockType.COMMITTED) {
                block = blobUncommittedBlocks.get(blockReference.blockId);
                if (block != null) {
                    consumedUncommittedBlockIds.add(blockReference.blockId);
                }
            }
            if (block == null && blockReference.type != BlockType.UNCOMMITTED) {
                block = committedBlocks.get(blockReference.blockId);
            }
            if (block == null) {
                throw new InvalidBlockListException();
            }
            try {
                block.writeTo(contents);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            newCommittedBlocks.put(blockReference.blockId, block);
        }

        if (consumedUncommittedBlockIds.isEmpty() == false) {
            final Map<String, BytesReference> remainingBlocks = uncommittedBlocks.get(blobPath);
            consumedUncommittedBlockIds.forEach(remainingBlocks::remove);
            if (remainingBlocks.isEmpty()) {
                uncommittedBlocks.remove(blobPath);
            }
        }
        return new Blob(new BytesArray(contents.toByteArray()), metadata, nextETag(), newCommittedBlocks);
    }

    private String nextETag() {
        return "\"" + ++nextETag + "\"";
    }

    private static StorageError conditionError(final Headers headers, final Blob currentBlob) {
        final String ifMatch = headers.getFirst("If-Match");
        if (ifMatch != null && (currentBlob == null || matchesETag(ifMatch, currentBlob.eTag) == false)) {
            return new StorageError(RestStatus.PRECONDITION_FAILED, "ConditionNotMet");
        }

        final String ifNoneMatch = headers.getFirst("If-None-Match");
        if (ifNoneMatch != null && currentBlob != null && matchesETag(ifNoneMatch, currentBlob.eTag)) {
            if ("*".equals(ifNoneMatch.trim())) {
                return new StorageError(RestStatus.CONFLICT, "BlobAlreadyExists");
            }
            return new StorageError(RestStatus.PRECONDITION_FAILED, "ConditionNotMet");
        }
        return null;
    }

    private static boolean matchesETag(final String condition, final String eTag) {
        return Arrays.stream(condition.split(",")).map(String::trim).anyMatch(value -> "*".equals(value) || eTag.equals(value));
    }

    private static Map<String, String> readMetadata(final Headers headers) {
        final Map<String, String> metadata = new HashMap<>();
        headers.forEach((name, values) -> {
            final String lowerCaseName = name.toLowerCase(Locale.ROOT);
            if (lowerCaseName.startsWith("x-ms-meta-") && values.isEmpty() == false) {
                metadata.put(lowerCaseName.substring("x-ms-meta-".length()), values.get(0));
            }
        });
        return metadata;
    }

    private static void addCommittedBlobHeaders(final Headers headers, final Blob blob) {
        headers.add("ETag", blob.eTag);
        blob.metadata.forEach((name, value) -> headers.add("x-ms-meta-" + name, value));
    }

    private enum BlockType {
        COMMITTED("Committed"),
        UNCOMMITTED("Uncommitted"),
        LATEST("Latest");

        private final String element;

        BlockType(final String element) {
            this.element = element;
        }

        private static BlockType fromElement(final String element) {
            return Arrays.stream(values()).filter(type -> type.element.equals(element)).findFirst().orElse(null);
        }
    }

    private static class BlockReference {
        private final BlockType type;
        private final String blockId;

        private BlockReference(final BlockType type, final String blockId) {
            this.type = type;
            this.blockId = blockId;
        }
    }

    private static class Blob {
        private final BytesReference contents;
        private final Map<String, String> metadata;
        private final String eTag;
        private final Map<String, BytesReference> committedBlocks;

        private Blob(
            final BytesReference contents,
            final Map<String, String> metadata,
            final String eTag,
            final Map<String, BytesReference> committedBlocks
        ) {
            this.contents = contents;
            this.metadata = Collections.unmodifiableMap(new HashMap<>(metadata));
            this.eTag = eTag;
            this.committedBlocks = Collections.unmodifiableMap(new HashMap<>(committedBlocks));
        }
    }

    private static class StorageError {
        private final RestStatus status;
        private final String code;

        private StorageError(final RestStatus status, final String code) {
            this.status = status;
            this.code = code;
        }
    }

    private static class InvalidBlockListException extends Exception {}
}
