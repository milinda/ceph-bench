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

public abstract class S3Client {
  @Parameter(names = {"-h", "--host"}, description = "Ceph Object Gateway host", required = true)
  String host;

  @Parameter(names = {"-k", "--access-key"}, required = true, description = "Access Key")
  String accessKey;

  @Parameter(names = {"-s", "--secret-key"}, required = true, description = "Access Key Secret")
  String secretKey;

  AmazonS3 getS3Connection() {
    AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);

    AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
    conn.setEndpoint(host);

    return conn;
  }

  boolean isBucketExists(String bucket) {
    return getS3Connection().doesBucketExist(bucket);
  }

  void createBucket(String bucket) {
    getS3Connection().createBucket(bucket);
  }

  void deleteBucket(String bucket) {
    getS3Connection().deleteBucket(bucket);
  }

}
