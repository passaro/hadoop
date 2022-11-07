/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import static org.apache.hadoop.fs.s3a.Constants.*;

import com.amazonaws.services.s3.AmazonS3;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Abstract base class for S3A unit tests using a mock S3 client and a null
 * metadata store.
 */
public abstract class AbstractS3AMockTest {

  protected static final String BUCKET = "mock-bucket";
  protected static final AwsServiceException NOT_FOUND =
      AwsServiceException.builder()
          .message("Not Found")
          .statusCode(404)
          .awsErrorDetails(AwsErrorDetails.builder()
              .errorCode("")
              .build())
          .build();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  protected S3AFileSystem fs;
  protected AmazonS3 s3;
  protected S3Client s3V2;

  @Before
  public void setup() throws Exception {
    Configuration conf = createConfiguration();
    fs = new S3AFileSystem();
    URI uri = URI.create(FS_S3A + "://" + BUCKET);
    // unset S3CSE property from config to avoid pathIOE.
    conf.unset(Constants.S3_ENCRYPTION_ALGORITHM);
    fs.initialize(uri, conf);
    s3 = fs.getAmazonS3ClientForTesting("mocking");
    s3V2 = fs.getAmazonS3V2ClientForTesting("mocking");
  }

  @SuppressWarnings("deprecation")
  public Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.setClass(S3_CLIENT_FACTORY_IMPL, MockS3ClientFactory.class,
        S3ClientFactory.class);

    // use minimum multipart size for faster triggering
    conf.setLong(Constants.MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.setInt(Constants.S3A_BUCKET_PROBE, 1);
    // this is so stream draining is always blocking, allowing
    // assertions to be safely made without worrying
    // about any race conditions
    conf.setInt(ASYNC_DRAIN_THRESHOLD, Integer.MAX_VALUE);
    return conf;
  }

  public S3Client getS3Client() {
    return s3V2;
  }

  @After
  public void teardown() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }
}
