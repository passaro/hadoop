/*
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

package org.apache.hadoop.fs.s3a.statistics.impl;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import org.apache.hadoop.fs.s3a.statistics.StatisticsFromAwsSdk;

/**
 * Collect statistics from the AWS SDK and forward to an instance of
 * {@link StatisticsFromAwsSdk} and thence into the S3A statistics.
 * <p>
 * See {@code com.facebook.presto.hive.s3.PrestoS3FileSystemMetricCollector}
 * for the inspiration for this.
 * <p>
 * See {@code software.amazon.awssdk.core.metrics.CoreMetric} for metric names.
 */
public class AwsStatisticsCollector implements MetricPublisher {

  /**
   * final destination of updates.
   */
  private final StatisticsFromAwsSdk collector;

  /**
   * Instantiate.
   * @param collector final destination of updates
   */
  public AwsStatisticsCollector(final StatisticsFromAwsSdk collector) {
    this.collector = collector;
  }

  /**
   * This is the callback from the AWS SDK where metrics
   * can be collected.
   * @param request AWS request
   * @param response AWS response
   */
  @Override
  public void publish(MetricCollection metricCollection) {
    final long[] throttling = {0};
    recurseThroughChildren(metricCollection)
        .collect(Collectors.toList())
        .forEach(m -> {
      counter(m, CoreMetric.RETRY_COUNT, retries -> {
        // Replaces com.amazonaws.util.AWSRequestMetrics.Field.HttpClientRetryCount
        collector.updateAwsRetryCount(retries);

        // Replaces com.amazonaws.util.AWSRequestMetrics.Field.RequestCount (always HttpClientRetryCount+1)
        collector.updateAwsRequestCount(retries + 1);
      });

      // TODO: confirm replacement
      // Replaces com.amazonaws.util.AWSRequestMetrics.Field.ThrottleException
      counter(m, HttpMetric.HTTP_STATUS_CODE, statusCode -> {
        if (statusCode == HttpStatusCode.THROTTLING) {
          throttling[0] += 1;
        }
      });

      // Replaces com.amazonaws.util.AWSRequestMetrics.Field.ClientExecuteTime
      timing(m, CoreMetric.API_CALL_DURATION,
          collector::noteAwsClientExecuteTime);

      // Replaces com.amazonaws.util.AWSRequestMetrics.Field.HttpRequestTime
      timing(m, CoreMetric.SERVICE_CALL_DURATION,
          collector::noteAwsRequestTime);

      // Replaces com.amazonaws.util.AWSRequestMetrics.Field.RequestMarshallTime
      timing(m, CoreMetric.MARSHALLING_DURATION,
          collector::noteRequestMarshallTime);

      // Replaces com.amazonaws.util.AWSRequestMetrics.Field.RequestSigningTime
      timing(m, CoreMetric.SIGNING_DURATION,
          collector::noteRequestSigningTime);

      // TODO: confirm replacement
      // Replaces com.amazonaws.util.AWSRequestMetrics.Field.ResponseProcessingTime
      timing(m, CoreMetric.UNMARSHALLING_DURATION,
          collector::noteResponseProcessingTime);
    });

    collector.updateAwsThrottleExceptionsCount(throttling[0]);
  }

  @Override
  public void close() {

  }

  /**
   * Process a timing.
   * @param collection metric collection
   * @param metric metric
   * @param durationConsumer consumer
   */
  private void timing(
      MetricCollection collection,
      SdkMetric<Duration> metric,
      Consumer<Duration> durationConsumer) {
    collection
        .metricValues(metric)
        .forEach(v -> durationConsumer.accept(v));
  }

  /**
   * Process a counter.
   * @param collection metric collection
   * @param metric metric
   * @param consumer consumer
   */
  private void counter(
      MetricCollection collection,
      SdkMetric<Integer> metric,
      LongConsumer consumer) {
    collection
        .metricValues(metric)
        .forEach(v -> consumer.accept(v.longValue()));
  }

  /**
   * Metric collections can be nested. Exposes a stream of the given
   * collection and its nested children.
   * @param metrics initial collection
   * @return a stream of all nested metric collections
   */
  private static Stream<MetricCollection> recurseThroughChildren(
      MetricCollection metrics) {
    return Stream.concat(
        Stream.of(metrics),
        metrics.children().stream()
            .flatMap(c -> recurseThroughChildren(c)));
  }
}
