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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClosableInputStreamHolder implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ClosableInputStreamHolder.class);

    private final List<InputStream> streams = new ArrayList<>();

    InputStream add(final InputStream is) {
        this.streams.add(is);
        return is;
    }

    @Override
    public void close() {
        for (final InputStream is : streams) {
            try {
                is.close();
            } catch (final IOException e) {
                // We want to close all of them or, in case of an error closing some, as much as possible.
                // So no rethrowing, only logging.
                log.error("Error closing InputStream", e);
            }
        }
    }
}
