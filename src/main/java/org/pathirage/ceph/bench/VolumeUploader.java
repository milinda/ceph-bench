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

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class VolumeUploader extends S3Client {
  @Parameter(names = {"-vl", "--volume-list"}, description = "Path to volume list file", required = true)
  String volumeList;

  @Parameter(names = {"-h", "--host"}, description = "Ceph Object Gateway host", required = true)
  String host;

  @Parameter(names = {"-k", "--access-key"}, required = true, description = "Access Key")
  String accessKey;

  @Parameter(names = {"-s", "--secret-key"}, required = true, description = "Access Key Secret")
  String secretKey;

  @Parameter(names = {"-vd", "--volume-dir"}, description = "Path to directory containing volumes", required = true)
  private String volumesDirectory;

  @Parameter(names = {"-b", "--bucket"}, description = "Bucket Name")
  private String bucket = "volumes";

  private final ExecutorService executorService = Executors.newFixedThreadPool(10);

  private void uploadVolume(String volumeId, File volume) {
    getS3Connection().putObject(new PutObjectRequest(bucket, volumeId, volume));
  }

  public void upload() throws IOException, InterruptedException {
    List<String> volumes = Files.lines(Paths.get(volumeList)).collect(Collectors.toList());
    if (!isBucketExists(bucket)) {
      createBucket(bucket);
    }

    CountDownLatch doneSignal = new CountDownLatch(volumes.size());

    for (String volume : volumes) {
      executorService.submit(() -> {
        uploadVolume(volume, Paths.get(volumesDirectory, String.format("%s.zip", volume)).toFile());
      });
    }

    doneSignal.await();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    VolumeUploader uploader = new VolumeUploader();
    new JCommander(uploader, args);
    uploader.upload();
  }
}
