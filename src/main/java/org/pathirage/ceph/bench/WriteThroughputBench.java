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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WriteThroughputBench extends AbstractBenchmark {

  @Parameter(names = {"-vd", "--volume-dir"}, description = "Path to directory containing volumes", required = true)
  private String volumesDirectory;

  private void writeVolumes(List<String> volumes) {
    for (String volume : volumes) {
      pool.submit(() -> {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);

        AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
        conn.setEndpoint(host);
        // When saved to a file, we assume that node running the benchmark has better write throughput than read throughput of Ceph
        conn.putObject(new PutObjectRequest(VOL_BUCKET, volume, Paths.get(volumesDirectory, String.format("%s.zip", volume)).toFile()));

        doneSignal.countDown();
      });
    }
  }

  public void run() throws InterruptedException {
    for (int i = 0; i < iterations; i++) {
      if (isBucketExists(VOL_BUCKET_WRITE)) {
        deleteBucket(VOL_BUCKET_WRITE);
      }

      createBucket(VOL_BUCKET_WRITE);

      long start = System.currentTimeMillis();

      List<String> volumes = getRandomVolumes(numberOfVolumes);
      writeVolumes(volumes);

      doneSignal.await();

      long stop = System.currentTimeMillis();

      long size = sizeOfVolumes(volumes, new File(volumesDirectory));

      throughput.recordValue(size / (stop - start));

      // Resetting latch
      doneSignal = new CountDownLatch(numberOfVolumes);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    WriteThroughputBench writeThroughputBench = new WriteThroughputBench();
    new JCommander(writeThroughputBench, args);
    writeThroughputBench.init();
    writeThroughputBench.run();
    writeThroughputBench.printResults();
  }

  @Override
  String benchmark() {
    return "write-throughput";
  }
}
