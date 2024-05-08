/*
 * Copyright 2023 Aiven Oy
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
package io.aiven.kafka.tieredstorage.storage;

import java.time.Duration;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;

public class RLMQuotaManager {


	private static volatile Bucket limitBucket;

	public static void createBucket(Long uploadLimit) {
		if (limitBucket == null) {
			synchronized (RLMQuotaManager.class) {
				if (limitBucket == null) {
					if (uploadLimit == null) {
						uploadLimit = Long.MAX_VALUE;
					}
					final Bandwidth bandwidth = Bandwidth.simple(uploadLimit, Duration.ofSeconds(1));
					limitBucket = Bucket4j.builder().addLimit(bandwidth).build();
				}
			}
		}
	}

	public static Bucket getBucket() {
		if (limitBucket == null) {
			synchronized (RLMQuotaManager.class) {
				if (limitBucket == null) {
					final Bandwidth bandwidth = Bandwidth.simple(Long.MAX_VALUE, Duration.ofSeconds(1));
					limitBucket = Bucket4j.builder().addLimit(bandwidth).build();
				}
			}
		}
		return limitBucket;
	}

}
