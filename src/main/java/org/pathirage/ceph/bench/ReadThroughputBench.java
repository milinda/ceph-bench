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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ReadThroughputBench {

  @Parameter(names = {"-n", "--num-vols"}, description = "Number Of Volumes To Read")
  private Integer numberOfVolumes = 100;

  @Parameter(names = {"-vl", "--volume-list"}, description = "Path to volume list file", required = true)
  private String volumeList;

  @Parameter(names = {"-h", "--host"}, description = "Ceph Object Gateway host", required = true)
  private String host;

  @Parameter(names = {"-k", "--access-key"}, required = true, description = "Access Key")
  private String accessKey;

  @Parameter(names = {"-s", "--secret-key"}, required = true, description = "Access Key Secret")
  private String secretKey;

  @Parameter(names = {"-i", "--iterations"}, description = "Number of times the test should be performed")
  private Integer iterations = 5;

  @Parameter(names = {"-p", "--parallelism"}, description = "Worker parallelism")
  private Integer parallelism = 5;

  private List<String> volumes;

  private ExecutorService pool;

  private CountDownLatch doneSignal;

  private Histogram throughput = new Histogram(5);

  private final Path parentDir = Files.createTempDirectory("readthroughput");

  public ReadThroughputBench() throws IOException {
  }


  private void init() throws IOException {
    pool = Executors.newFixedThreadPool(parallelism);
    doneSignal = new CountDownLatch(numberOfVolumes);
    loadVolumes();
  }

  private void loadVolumes() throws IOException {
    volumes = Files.lines(Paths.get(volumeList)).collect(Collectors.toList());
  }

  private List<String> getRandomVolumes(int count) {
    Collections.shuffle(volumes);

    List<String> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(volumes.get(i));
    }

    return result;
  }

  private void downloadVolumes(List<String> volumes, final Path location) {
    for (String volume : volumes) {
      pool.submit(() -> {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);

        AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
        conn.setEndpoint(host);
        // When saved to a file, we assume that node running the benchmark has better write throughput than read throughput of Ceph
        conn.getObject(new GetObjectRequest("volumes", volume), Paths.get(parentDir.toAbsolutePath().toString(), volume).toFile());

        doneSignal.countDown();
      });
    }
  }

  private long calculateDownloadedBytes(File directory) {
    long length = 0;
    for (File file : directory.listFiles()) {
      if (file.isFile())
        length += file.length();
      else
        length += calculateDownloadedBytes(file);
    }
    return length;
  }

  public void run() throws InterruptedException, IOException {
    for (int i = 0; i < iterations; i++) {
      long start = System.currentTimeMillis();

      downloadVolumes(getRandomVolumes(numberOfVolumes), parentDir);

      // Wait for downloads to finish
      doneSignal.await();

      long stop = System.currentTimeMillis();

      long size = calculateDownloadedBytes(parentDir.toFile());

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
  }
}
