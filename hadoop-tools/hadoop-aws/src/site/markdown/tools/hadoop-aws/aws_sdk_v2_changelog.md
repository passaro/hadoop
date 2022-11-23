<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Upgrade S3A to AWS SDK V2: Changelog

This document tracks changes to S3A during the upgrade to AWS SDK V2. Once the upgrade
is complete, some of its content will be added to the existing document 
[Upcoming upgrade to AWS Java SDK V2](./aws_sdk_upgrade.html).

This work is tracked in [HADOOP-18073](https://issues.apache.org/jira/browse/HADOOP-18073).

## Auditing

The SDK v2 offers a new `ExecutionInterceptor` 
[interface](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html) 
which broadly replaces the `RequestHandler2` abstract class from v1. 
Switching to the new mechanism in S3A brings:

* Simplification in `AWSAuditEventCallbacks` (and implementors) which can now extend
  `ExecutionInterceptor`
* "Registering" a Span with a request has moved from `requestCreated` to `beforeExecution` 
  (where an `ExecutionAttributes` instance is first available)
* The ReferrerHeader is built and added to the http request in `modifyHttpRequest`,
  rather than in `beforeExecution`, where no http request is yet available
* Dynamic loading of interceptors has been implemented to reproduce previous behaviour
  with `RequestHandler2`s. The AWS SDK v2 offers an alternative mechanism, described
  [here](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html)
  under "Interceptor Registration", which could make it redundant.

In the Transfer Manager, `TransferListener` replaces `TransferStateChangeListener`. S3A code
has been updated, but registration of the new listeners is currently commented out because 
it causes an incompatibility issue with the internal logger, resulting in `NoSuchMethodError`s,
at least in the current TransferManager Preview release. 


## Metric Collection

`AwsStatisticsCollector` has been updated to implement the new `MetricPublisher` interface
and collect the metrics from a `MetricCollection` object.
The following table maps SDK v2 metrics to their equivalent in v1:

| v2 Metrics	                                                 | com.amazonaws.util.AWSRequestMetrics.Field	 | Comment	                       |
|-------------------------------------------------------------|---------------------------------------------|--------------------------------|
| CoreMetric.RETRY_COUNT	                                     | HttpClientRetryCount	                       | 	                              |
| CoreMetric.RETRY_COUNT	                                     | RequestCount	                               | always HttpClientRetryCount+1	 |
| HttpMetric.HTTP_STATUS_CODE with HttpStatusCode.THROTTLING	 | ThrottleException	                          | to be confirmed	               |
| CoreMetric.API_CALL_DURATION	                               | ClientExecuteTime	                          | 	                              |
| CoreMetric.SERVICE_CALL_DURATION	                           | HttpRequestTime	                            | 	                              |
| CoreMetric.MARSHALLING_DURATION	                            | RequestMarshallTime	                        | 	                              |
| CoreMetric.SIGNING_DURATION	                                | RequestSigningTime	                         | 	                              |
| CoreMetric.UNMARSHALLING_DURATION	                          | ResponseProcessingTime	                     | to be confirmed	               |

Note that none of the timing metrics (`*_DURATION`) are currently collected in S3A.


## DeleteObject

In SDK v2, bulk delete does not throw on partial failure: the response contains a list of errors. 
A new `MultiObjectDeleteException` class was introduced and is thrown when appropriate to
reproduce the previous behaviour.
* `MultiObjectDeleteSupport.translateDeleteException` was moved into `MultiObjectDeleteException`.
* `ObjectIdentifier` replaces DeleteObjectsRequest.KeyVersion.


## Exception Handling

The code to handle exceptions thrown by the SDK has been updated to reflect the changes in v2: 

* `com.amazonaws.SdkBaseException` and `com.amazonaws.AmazonClientException` changes:
  * These classes have combined and replaced with 
    `software.amazon.awssdk.core.exception.SdkException`.
* `com.amazonaws.SdkClientException` changes:
  * This class has been replaced with `software.amazon.awssdk.core.exception.SdkClientException`.
  * This class now extends `software.amazon.awssdk.core.exception.SdkException`.
* `com.amazonaws.AmazonServiceException` changes:
  * This class has been replaced with 
    `software.amazon.awssdk.awscore.exception.AwsServiceException`.
  * This class now extends `software.amazon.awssdk.core.exception.SdkServiceException`, 
    a new exception type that extends `software.amazon.awssdk.core.exception.SdkException`.

See also the 
[SDK changelog](https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#3-exception-changes).


## Failure Injection

While using the SDK v1, failure injection was implemented in `InconsistentAmazonS3CClient`, 
which extended the S3 client. In SDK v2, reproducing this approach would not be straightforward, 
since the default S3 client is an internal final class. Instead, the same fault injection strategy 
is now performed by a `FailureInjectionInterceptor` (see 
[ExecutionInterceptor](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html)) 
registered on the default client by `InconsistentS3CClientFactory`. 
`InconsistentAmazonS3CClient` has been removed. No changes to the user configuration are required.


## Select

In SDK v2, Handling of select requests has changes significantly since SelectObjectContent is 
only supported on the new async S3 client. In previous versions, the response to a 
SelectObjectContent request exposed the results in a `SelectRecordsInputStream`, which S3A 
could wrap in `SelectInputStream`. In v2, instead, the response needs to be handled by an object
implementing `SelectObjectContentResponseHandler`, which can receive an async publisher of
the "events" returned by the service (`SdkPublisher<SelectObjectContentEventStream>`). 

In order to adapt the new API in S3A, three new classes have been introduced in 
`org.apache.hadoop.fs.s3a.select`:

* `SelectObjectContentHelper`: wraps the `selectObjectContent()` call, provides a custom 
  response handler to receive the response, and exposes a `SelectEventStreamPublisher`. 
* `SelectEventStreamPublisher`: a publisher of select event stream events, which handles the 
  future returned by the select call and wraps the original publisher. This class provides
  a `toRecordsInputStream()` method which returns an input stream containing the results, 
  reproducing the behaviour of the old `SelectRecordsInputStream`.
* `BlockingEnumeration`: an adapter which consumes elements received from a publisher and
  exposes them through an `Enumeration` interface. Used in 
  `SelectEventStreamPublisher.toRecordsInputStream()` to adapt the event publisher into
  an enumeration of input streams, eventually passed to a `SequenceInputStream`.


## GetObject

* Previously, `S3AInputStream` had to keep a reference to the `S3Object` while holding the wrapped
  `S3ObjectInputStream`. When upgraded to SDK v2, it can simply wrap new 
  `ResponseInputStream<GetObjectResponse>`, which handles lifetime correctly. Same applies 
  to `SDKStreamDrainer`. Furthermore, the map in `S3ARemoteObject` associating input streams and
  `S3Object` instances is no longer needed.
* The range header on a `GetObject` request is now specified as a string, rather than a 
  `start`-`end` pair. `S3AUtils.formatRange` was introduced to format it.
