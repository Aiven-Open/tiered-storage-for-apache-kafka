/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.utils.Utils;

import io.aiven.kafka.tieredstorage.iceberg.manifest.DataFileMetadata;

import org.apache.avro.generic.GenericData;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * Logically concatenates multiple Parquet readers.
 */
public class MultiFileReader implements Closeable {
    private final Iterator<DataFileMetadata> fileLocations;
    private final ReaderFactory readerFactory;

    private CloseableIterable<GenericData.Record> reader = null;
    private CloseableIterator<GenericData.Record> recordIterator = null;

    public MultiFileReader(final List<DataFileMetadata> files,
                           final ReaderFactory readerFactory) {
        Objects.requireNonNull(files, "files cannot be null");
        this.fileLocations = new ArrayList<>(files).iterator();
        this.readerFactory = Objects.requireNonNull(readerFactory, "readerFactory cannot be null");
    }

    GenericData.Record read() throws IOException {
        if (reader == null && !initNextReader()) {
            return null;
        }

        while (!recordIterator.hasNext()) {
            if (!initNextReader()) {
                return null;
            }
        }
        return recordIterator.next();
    }

    private boolean initNextReader() throws IOException {
        if (this.reader != null) {
            Utils.closeQuietly(this.reader, "reader");
            this.reader = null;
        }

        if (!fileLocations.hasNext()) {
            return false;
        } else {
            this.reader = readerFactory.create(fileLocations.next());
            this.recordIterator = reader.iterator();
            return true;
        }
    }


    @Override
    public void close() throws IOException {
        Utils.closeQuietly(reader, "reader");
        this.reader = null;
    }

    @FunctionalInterface
    public interface ReaderFactory {
        CloseableIterable<GenericData.Record> create(DataFileMetadata dataFileMetadata) throws IOException;
    }
}
