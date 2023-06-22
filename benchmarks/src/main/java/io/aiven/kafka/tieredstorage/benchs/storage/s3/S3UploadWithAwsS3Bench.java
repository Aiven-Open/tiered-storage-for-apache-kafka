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

package io.aiven.kafka.tieredstorage.benchs.storage.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class S3UploadWithAwsS3Bench extends S3UploadBench {

    @Override
    AmazonS3 s3() {
        return AmazonS3ClientBuilder
            .standard()
//            .withRegion(System.getenv("AWS_REGION"))
            .build();
    }

    @Setup(Level.Trial)
    public void setup() {
        s3Client = s3();

        super.setup();
    }

    @TearDown
    public void teardown() {
        super.teardown();
    }


    public static void main(final String[] args) throws Exception {
        final Options opts = new OptionsBuilder()
            .include(S3UploadWithAwsS3Bench.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "output=flamegraph")
            .build();
        new Runner(opts).run();
    }
}
