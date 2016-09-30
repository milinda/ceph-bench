/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pathirage.ceph.bench;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.beust.jcommander.JCommander;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ReadThroughputBench extends AbstractBenchmark {

  private final Path parentDir = Files.createTempDirectory("readthroughput");

  public ReadThroughputBench() throws IOException {
  }

  private void downloadVolumes(List<String> volumes) {
    for (String volume : volumes) {
      pool.submit(() -> {
        // When saved to a file, we assume that node running the benchmark has better write throughput than read throughput of Ceph
        getS3Connection().getObject(new GetObjectRequest(VOL_BUCKET, volume), Paths.get(parentDir.toAbsolutePath().toString(), volume).toFile());

        doneSignal.countDown();
      });
    }
  }

  public void run() throws InterruptedException, IOException {
    if (!isBucketExists(VOL_BUCKET)) {
      throw new RuntimeException("Cannot find bucket " + VOL_BUCKET + " containing volumes.");
    }

    for (int i = 0; i < iterations; i++) {
      long start = System.currentTimeMillis();

      downloadVolumes(getRandomVolumes(numberOfVolumes));

      // Wait for downloads to finish
      doneSignal.await();

      long stop = System.currentTimeMillis();

      long size = directorySize(parentDir.toFile());

      throughput.recordValue(size / (stop - start));

      // Resetting latch
      doneSignal = new CountDownLatch(numberOfVolumes);
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    ReadThroughputBench readThroughputBench = new ReadThroughputBench();
    new JCommander(readThroughputBench, args);
    readThroughputBench.init();
    readThroughputBench.run();
    readThroughputBench.printResults();
  }

  @Override
  String benchmark() {
    return "read-throughput";
  }
}
