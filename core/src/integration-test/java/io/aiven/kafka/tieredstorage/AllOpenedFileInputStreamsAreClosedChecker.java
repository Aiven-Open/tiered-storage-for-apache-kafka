/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.verify;

/**
 * Checks that all {@link InputStream}s opened by
 * {@link Files#newInputStream(Path, OpenOption...)} are properly closed.
 *
 * <p>Should be used in try-with-resources.
 */
class AllOpenedFileInputStreamsAreClosedChecker implements AutoCloseable {
    private final MockedStatic<Files> mockedFiles;

    private final List<InputStream> opened = Collections.synchronizedList(new ArrayList<>());

    public AllOpenedFileInputStreamsAreClosedChecker() {
        this.mockedFiles = Mockito.mockStatic(Files.class, CALLS_REAL_METHODS);
        this.mockedFiles.when(() -> Files.newInputStream(any(Path.class)))
            .thenAnswer(invocation -> {
                final InputStream spy = Mockito.spy((InputStream) invocation.callRealMethod());
                opened.add(spy);
                return spy;
            });
    }

    @Override
    public void close() throws IOException {
        mockedFiles.close();

        assert !opened.isEmpty();
        for (final InputStream spy : opened) {
            verify(spy).close();
        }
    }
}
