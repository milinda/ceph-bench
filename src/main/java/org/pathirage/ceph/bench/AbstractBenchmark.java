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
import com.beust.jcommander.Parameter;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class AbstractBenchmark extends S3Client {
  static final String VOL_BUCKET = "volumes";
  static final String VOL_BUCKET_WRITE = "volumesw";
  static final double[] LOGARITHMIC_PERCENTILES = {
      0.0f,
      10.0f,
      20.0f,
      30.0f,
      40.0f,
      50.0f,
      55.0f,
      60.0f,
      65.0f,
      70.0f,
      75.0f,
      77.5f,
      80.0f,
      82.5f,
      85.0f,
      87.5f,
      88.75f,
      90.0f,
      91.25f,
      92.5f,
      93.75f,
      94.375f,
      95.0f,
      95.625f,
      96.25f,
      96.875f,
      97.1875f,
      97.5f,
      97.8125f,
      98.125f,
      98.4375f,
      98.5938f,
      98.75f,
      98.9062f,
      99.0625f,
      99.2188f,
      99.2969f,
      99.375f,
      99.4531f,
      99.5313f,
      99.6094f,
      99.6484f,
      99.6875f,
      99.7266f,
      99.7656f,
      99.8047f,
      99.8242f,
      99.8437f,
      99.8633f,
      99.8828f,
      99.9023f,
      99.9121f,
      99.9219f,
      99.9316f,
      99.9414f,
      99.9512f,
      99.9561f,
      99.9609f,
      99.9658f,
      99.9707f,
      99.9756f,
      99.978f,
      99.9805f,
      99.9829f,
      99.9854f,
      99.9878f,
      99.989f,
      99.9902f,
      99.9915f,
      99.9927f,
      99.9939f,
      99.9945f,
      99.9951f,
      99.9957f,
      99.9963f,
      99.9969f,
      99.9973f,
      99.9976f,
      99.9979f,
      99.9982f,
      99.9985f,
      99.9986f,
      99.9988f,
      99.9989f,
      99.9991f,
      99.9992f,
      99.9993f,
      99.9994f,
      99.9995f,
      99.9996f,
      99.9997f,
      99.9998f,
      99.9999f,
      100.0f};


  @Parameter(names = {"-n", "--num-vols"}, description = "Number Of Volumes To Read")
  Integer numberOfVolumes = 100;

  @Parameter(names = {"-vl", "--volume-list"}, description = "Path to volume list file", required = true)
  String volumeList;

  @Parameter(names = {"-i", "--iterations"}, description = "Number of times the test should be performed")
  Integer iterations = 5;

  @Parameter(names = {"-p", "--parallelism"}, description = "Worker parallelism")
  Integer parallelism = 5;

  List<String> volumes;

  ExecutorService pool;

  CountDownLatch doneSignal;

  Histogram throughput = new Histogram(5);

  public void init() throws IOException {
    pool = Executors.newFixedThreadPool(parallelism);
    doneSignal = new CountDownLatch(numberOfVolumes);
    loadVolumes();
  }

  private void loadVolumes() throws IOException {
    volumes = Files.lines(Paths.get(volumeList)).collect(Collectors.toList());
  }

  List<String> getRandomVolumes(int count) {
    Collections.shuffle(volumes);

    List<String> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(volumes.get(i));
    }

    return result;
  }

  long directorySize(File directory) {
    long length = 0;
    for (File file : directory.listFiles()) {
      if (file.isFile())
        length += file.length();
      else
        length += directorySize(file);
    }
    return length;
  }

  long sizeOfVolumes(List<String> volumes, File parentDir) {
    long length = 0;
    for (String volume : volumes) {
      length += new File(parentDir, String.format("%s.zip", volume)).length();
    }

    return length;
  }

  abstract String benchmark();

  public void printResults() {
    System.out.println("Benchmark: \t" + benchmark());
    System.out.println("Time: \t" + new Date());
    System.out.println("Volume Count: \t" + numberOfVolumes);
    System.out.println("Iterations:\t" + iterations);
    System.out.println("Parallelism:\t" + parallelism);
    System.out.println("Percentile \t Value");
    for (double p : LOGARITHMIC_PERCENTILES) {
      System.out.println(String.format("%f \t %f", p, throughput.getValueAtPercentile(p)));
    }
  }
}
