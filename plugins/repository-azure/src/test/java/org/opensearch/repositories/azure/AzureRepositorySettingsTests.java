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

package org.opensearch.repositories.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.ParallelTransferOptions;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;

import java.util.List;

import reactor.core.scheduler.Schedulers;
import reactor.netty.http.HttpResources;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzureRepositorySettingsTests extends OpenSearchTestCase {
    @AfterClass
    public static void shutdownSchedulers() {
        HttpResources.disposeLoopsAndConnections();
        Schedulers.shutdownNow();
    }

    private AzureRepository azureRepository(Settings settings) {
        return azureRepository(settings, BlobClient.BLOB_DEFAULT_UPLOAD_BLOCK_SIZE, ByteSizeUnit.MB.toBytes(256));
    }

    private AzureRepository azureRepository(Settings settings, long writeBlockSize, long maxSingleUploadSize) {
        Settings internalSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put(settings)
            .build();
        final AzureStorageService storageService = mock(AzureStorageService.class);
        when(storageService.getEffectiveBlobRequestOptionsForWriteBlob("default")).thenReturn(
            new ParallelTransferOptions().setBlockSizeLong(writeBlockSize).setMaxSingleUploadSizeLong(maxSingleUploadSize)
        );
        final AzureRepository azureRepository = new AzureRepository(
            new RepositoryMetadata("foo", "azure", internalSettings),
            NamedXContentRegistry.EMPTY,
            storageService,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        assertThat(azureRepository.getBlobStore(), is(nullValue()));
        return azureRepository;
    }

    public void testReadonlyDefault() {
        assertThat(azureRepository(Settings.EMPTY).isReadOnly(), is(false));
    }

    public void testReadonlyDefaultAndReadonlyOn() {
        assertThat(azureRepository(Settings.builder().put("readonly", true).build()).isReadOnly(), is(true));
    }

    public void testReadonlyWithPrimaryOnly() {
        assertThat(
            azureRepository(
                Settings.builder().put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name()).build()
            ).isReadOnly(),
            is(false)
        );
    }

    public void testReadonlyWithPrimaryOnlyAndReadonlyOn() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_ONLY.name())
                    .put("readonly", true)
                    .build()
            ).isReadOnly(),
            is(true)
        );
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOn() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
                    .put("readonly", true)
                    .build()
            ).isReadOnly(),
            is(true)
        );
    }

    public void testReadonlyWithSecondaryOnlyAndReadonlyOff() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.SECONDARY_ONLY.name())
                    .put("readonly", false)
                    .build()
            ).isReadOnly(),
            is(false)
        );
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOn() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
                    .put("readonly", true)
                    .build()
            ).isReadOnly(),
            is(true)
        );
    }

    public void testReadonlyWithPrimaryAndSecondaryOnlyAndReadonlyOff() {
        assertThat(
            azureRepository(
                Settings.builder()
                    .put(AzureRepository.Repository.LOCATION_MODE_SETTING.getKey(), LocationMode.PRIMARY_THEN_SECONDARY.name())
                    .put("readonly", false)
                    .build()
            ).isReadOnly(),
            is(false)
        );
    }

    public void testChunkSize() {
        // default chunk size
        AzureRepository azureRepository = azureRepository(Settings.EMPTY);
        assertEquals(AzureStorageService.MAX_CHUNK_SIZE, azureRepository.chunkSize());

        // chunk size in settings
        int size = randomIntBetween(1, 256);
        azureRepository = azureRepository(Settings.builder().put("chunk_size", size + "mb").build());
        assertEquals(new ByteSizeValue(size, ByteSizeUnit.MB), azureRepository.chunkSize());

        // zero bytes is not allowed
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> azureRepository(Settings.builder().put("chunk_size", "0").build())
        );
        assertEquals("failed to parse value [0] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // negative bytes not allowed
        e = expectThrows(IllegalArgumentException.class, () -> azureRepository(Settings.builder().put("chunk_size", "-1").build()));
        assertEquals("failed to parse value [-1] for setting [chunk_size], must be >= [1b]", e.getMessage());

        // greater than max chunk size not allowed
        e = expectThrows(IllegalArgumentException.class, () -> azureRepository(Settings.builder().put("chunk_size", "6tb").build()));
        assertEquals(
            "failed to parse value [6tb] for setting [chunk_size], must be <= [" + AzureStorageService.MAX_CHUNK_SIZE.getStringRep() + "]",
            e.getMessage()
        );
    }

    public void testAzureBlockCountLimit() {
        azureRepository(Settings.builder().put("chunk_size", "50000b").build(), 1L, 1L);

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> azureRepository(Settings.builder().put("chunk_size", "50001b").build(), 1L, 1L)
        );
        assertEquals(
            "setting [azure.client.default.write.block_size] is [1b], but setting [chunk_size] [50001b] would require [50001] "
                + "blocks; Azure Block Blob supports at most [50000] blocks. Increase [azure.client.default.write.block_size] "
                + "to at least [2b] or reduce setting [chunk_size] to [50000b] or less",
            exception.getMessage()
        );
    }

    public void testMaximumChunkSizeBlockCountLimit() {
        final long maximumChunkSize = AzureStorageService.MAX_CHUNK_SIZE.getBytes();
        final long minimumWriteBlockSize = 1L + (maximumChunkSize - 1L) / 50_000L;

        azureRepository(Settings.EMPTY, minimumWriteBlockSize, 1L);

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> azureRepository(Settings.EMPTY, minimumWriteBlockSize - 1L, 1L)
        );
        assertThat(exception.getMessage(), containsString("setting [chunk_size] [" + maximumChunkSize + "b]"));
        assertThat(exception.getMessage(), containsString("to at least [" + minimumWriteBlockSize + "b]"));
    }

    public void testSingleUploadDoesNotUseBlockCountLimit() {
        azureRepository(Settings.builder().put("chunk_size", "50001b").build(), 1L, 50001L);
    }

    public void testSystemRepositoryDefault() {
        assertThat(azureRepository(Settings.EMPTY).isSystemRepository(), is(false));
    }

    public void testSystemRepositoryOn() {
        assertThat(azureRepository(Settings.builder().put("system_repository", true).build()).isSystemRepository(), is(true));
    }

    public void testRestrictedSettingsDefault() {
        List<Setting<?>> restrictedSettings = azureRepository(Settings.EMPTY).getRestrictedSystemRepositorySettings();
        assertThat(restrictedSettings.size(), is(5));
        assertTrue(restrictedSettings.contains(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING));
        assertTrue(restrictedSettings.contains(BlobStoreRepository.READONLY_SETTING));
        assertTrue(restrictedSettings.contains(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY));
        assertTrue(restrictedSettings.contains(AzureRepository.Repository.BASE_PATH_SETTING));
        assertTrue(restrictedSettings.contains(AzureRepository.Repository.LOCATION_MODE_SETTING));
    }
}
