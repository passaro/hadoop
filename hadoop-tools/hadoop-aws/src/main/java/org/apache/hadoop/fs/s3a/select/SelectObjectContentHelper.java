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

package org.apache.hadoop.fs.s3a.select;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.store.audit.AuditingFunctions.withinAuditSpan;
import static org.apache.hadoop.util.Preconditions.checkNotNull;

/**
 * Helper for SelectObjectContent queries against an S3 Bucket.
 * <p>
 * This API is for internal use only.
 * Span scoping: This helper is instantiated with span; it will be used
 * before operations which query S3
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class SelectObjectContentHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(SelectObjectContentHelper.class);

  /**
   * Owning filesystem.
   */
  private final S3AFileSystem owner;

  /**
   * Invoker for operations; uses the S3A retry policy and calls int
   * {@link #operationRetried(String, Exception, int, boolean)} on retries.
   */
  private final Invoker invoker;

  /** Configuration of the owner. This is a reference, not a copy. */
  private final Configuration conf;

  /** Bucket of the owner FS. */
  private final String bucket;

  /**
   * Store Context; extracted from owner.
   */
  private final StoreContext storeContext;

  /**
   * Audit Span.
   */
  private AuditSpan auditSpan;

  /**
   * Factory for AWS requests.
   */
  private final RequestFactory requestFactory;

  /**
   * Callbacks for this helper.
   */
  private final SelectObjectContentHelperCallbacks helperCallbacks;

  /**
   * Constructor.
   * @param owner owner FS creating the helper
   * @param conf Configuration object
   * @param auditSpan span to activate
   * @param SelectObjectContentHelperCallbacks callbacks used by SelectObjectContentHelper
   */
  public SelectObjectContentHelper(S3AFileSystem owner,
      Configuration conf,
      final AuditSpan auditSpan,
      final SelectObjectContentHelperCallbacks helperCallbacks) {
    this.owner = owner;
    this.invoker = new Invoker(new S3ARetryPolicy(conf),
        this::operationRetried);
    this.conf = conf;
    this.storeContext = owner.createStoreContext();
    this.bucket = owner.getBucket();
    this.auditSpan = checkNotNull(auditSpan);
    this.requestFactory = owner.getRequestFactory();
    this.helperCallbacks = helperCallbacks;
  }

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  void operationRetried(String text, Exception ex, int retries,
      boolean idempotent) {
    LOG.info("{}: Retried {}: {}", text, retries, ex.toString());
    LOG.debug("Stack", ex);
    owner.operationRetried(text, ex, retries, idempotent);
  }

  /**
   * Get the configuration of this instance; essentially the owning
   * filesystem configuration.
   * @return the configuration.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Create a S3 Select request builder for the destination path.
   * This does not build the query.
   * @param path pre-qualified path for query
   * @return the request builder
   */
  public SelectObjectContentRequest.Builder newSelectRequestBuilder(Path path) {
    try (AuditSpan span = auditSpan) {
      return requestFactory.newSelectRequestBuilder(
          storeContext.pathToKey(path));
    }
  }

  /**
   * Execute an S3 Select operation.
   * On a failure, the request is only logged at debug to avoid the
   * select exception being printed.
   *
   * @param source  source for selection
   * @param request Select request to issue.
   * @param action  the action for use in exception creation
   * @return response
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  public SelectEventStreamPublisher select(
      final Path source,
      final SelectObjectContentRequest request,
      final String action)
      throws IOException {
    // no setting of span here as the select binding is (statically) created
    // without any span.
    String bucketName = request.bucket();
    Preconditions.checkArgument(bucket.equals(bucketName),
        "wrong bucket: %s", bucketName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating select call {} {}",
          source, request.expression());
      LOG.debug(SelectBinding.toString(request));
    }
    return invoker.retry(
        action,
        source.toString(),
        true,
        withinAuditSpan(auditSpan, () -> {
          try (DurationInfo ignored =
              new DurationInfo(LOG, "S3 Select operation")) {
            try {
              return selectInternal(source, request, action);
            } catch (Throwable e) {
              LOG.error("Failure of S3 Select request against {}",
                  source);
              LOG.debug("S3 Select request against {}:\n{}",
                  source,
                  SelectBinding.toString(request),
                  e);
              throw e;
            }
          }
        }));
  }

  private SelectEventStreamPublisher selectInternal(
      Path source,
      SelectObjectContentRequest request,
      String action)
      throws IOException {
    try {
      Handler handler = new Handler();
      CompletableFuture<Void> selectOperationFuture =
          helperCallbacks.selectObjectContent(request, handler);
      return handler.eventPublisher(selectOperationFuture).join();
    } catch (Throwable e) {
      if (e instanceof CompletionException) {
        e = e.getCause();
      }
      IOException translated;
      if (e instanceof SdkException) {
        translated = S3AUtils.translateExceptionV2(action, source.toString(),
            (SdkException)e);
      } else {
        translated = new IOException(e);
      }
      throw translated;
    }
  }

  /**
   * Callbacks for SelectObjectContentHelper.
   */
  public interface SelectObjectContentHelperCallbacks {

    /**
     * Initiates a select request.
     * @param request selectObjectContent request
     * @param responseHandler response handler
     * @return future for the select operation
     */
    CompletableFuture<Void> selectObjectContent(
        SelectObjectContentRequest request,
        SelectObjectContentResponseHandler responseHandler);
  }

  private static class Handler implements SelectObjectContentResponseHandler {
    private volatile CompletableFuture<Pair<SelectObjectContentResponse,
        SdkPublisher<SelectObjectContentEventStream>>> responseAndPublisherFuture =
        new CompletableFuture<>();

    private volatile SelectObjectContentResponse response;

    public CompletableFuture<SelectEventStreamPublisher> eventPublisher(
        CompletableFuture<Void> selectOperationFuture) {
      return responseAndPublisherFuture.thenApply(p ->
          new SelectEventStreamPublisher(selectOperationFuture,
              p.getLeft(), p.getRight()));
    }

    @Override
    public void responseReceived(SelectObjectContentResponse response) {
      this.response = response;
    }

    @Override
    public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
      responseAndPublisherFuture.complete(Pair.of(response, publisher));
    }

    @Override
    public void exceptionOccurred(Throwable error) {
      responseAndPublisherFuture.completeExceptionally(error);
    }

    @Override
    public void complete() {
    }
  }
}
