/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromString;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Either;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class S3EventBridgeEventDetailValueOffloading
    implements EventBridgeEventDetailValueOffloadingStrategy {

  private static class DocumentContextAware<T> {
    final DocumentContext documentContext;
    final T value;

    DocumentContextAware(final DocumentContext documentContext, final T value) {
      this.documentContext = documentContext;
      this.value = value;
    }

    DocumentContextAware<T> documentContextWith(final T value) {
      return new DocumentContextAware<>(this.documentContext, value);
    }
  }

  private static final int SDK_TIMEOUT = 5000; // timeout in milliseconds for SDK calls
  private static final Configuration configuration =
      defaultConfiguration()
          .addOptions(
              SUPPRESS_EXCEPTIONS // suppress exception otherwise
              // com.jayway.jsonpath.ReadContext#read throws an exception if JSON path could not
              // be found
              );
  private static final Logger logger =
      ContextAwareLoggerFactory.getLogger(S3EventBridgeEventDetailValueOffloading.class);

  private final String bucketName;
  private final S3AsyncClient client;
  private final Supplier<UUID> idGenerator;

  private static final JsonPath jsonPathAdd = JsonPath.compile("$");

  private final JsonPath jsonPathRemove;
  private final Function<DocumentContext, DocumentContext> addDataRefPath;
  private final Function<DocumentContextAware<String>, DocumentContext> addDataRef;

  public S3EventBridgeEventDetailValueOffloading(
      final S3AsyncClient client,
      final String bucketName,
      final String jsonPathExp,
      final Supplier<UUID> idGenerator) {

    this.bucketName = bucketName;
    this.client = client;
    this.idGenerator = idGenerator;

    jsonPathRemove =
        validatedJsonPathOf(jsonPathExp)
            .map(
                S3EventBridgeEventDetailValueOffloading::throwIllegalArgumentException,
                jsonPath ->
                    // rewrite $.detail -> $,
                    // because PutEventsRequestEntry#detail is the sub document of $.detail
                    JsonPath.compile("$" + jsonPath.getPath().substring("$['detail']".length())));
    addDataRef =
        ctx ->
            ctx.documentContext
                .delete(jsonPathRemove)
                .put(jsonPathAdd, "dataref", format("arn:aws:s3:::%s/%s", bucketName, ctx.value));
    addDataRefPath = ctx -> ctx.put(jsonPathAdd, "datarefPath", jsonPathExp);
  }

  private static <T> T throwIllegalArgumentException(IllegalArgumentException e) {
    throw e;
  }

  private static Either<IllegalArgumentException, JsonPath> validatedJsonPathOf(String value) {
    JsonPath jsonPath;
    try {
      jsonPath = JsonPath.compile(value);
    } catch (Exception e) {
      return Either.left(new IllegalArgumentException(format("Invalid JSON Path '%s'.", value)));
    }

    if (!jsonPath.getPath().startsWith("$['detail']['value']")) {
      return Either.left(
          new IllegalArgumentException(
              format("JSON Path must start with '$.detail.value' but is '%s'.", value)));
    }
    if (!jsonPath.isDefinite()) {
      return Either.left(
          new IllegalArgumentException(
              format("JSON Path must be definite but '%s' is not.", value)));
    }
    return Either.right(jsonPath);
  }

  public static void validateJsonPathExp(String jsonPathExp) {
    validatedJsonPathOf(jsonPathExp)
        .map(S3EventBridgeEventDetailValueOffloading::throwIllegalArgumentException, identity());
  }

  @Override
  public EventBridgeEventDetailValueOffloadingResult apply(
      final List<MappedSinkRecord<PutEventsRequestEntry>> putEventsRequestEntries) {

    var result =
        putEventsRequestEntries.stream()
            .map(this::apply)
            .collect(partitioningBy(EventBridgeResult::isSuccess));

    var success = result.get(true).stream().map(EventBridgeResult::success).collect(toList());
    var errors = result.get(false).stream().map(EventBridgeResult::failure).collect(toList());

    return new EventBridgeEventDetailValueOffloadingResult(success, errors);
  }

  private EventBridgeResult<PutEventsRequestEntry> apply(
      final MappedSinkRecord<PutEventsRequestEntry> item) {

    var sinkRecord = item.getSinkRecord();
    var putEventsRequestEntry = item.getValue();

    var documentContext = JsonPath.parse(putEventsRequestEntry.detail(), configuration);

    return readJsonPathAsStringOptionalFrom(documentContext)
        .map(payload -> putS3(payload).mapRight(addDataRef))
        .orElse(Either.right(documentContext))
        .mapRight(addDataRefPath)
        .mapRight(DocumentContext::jsonString)
        .map(
            error -> failure(sinkRecord, error),
            detail -> success(sinkRecord, putEventsRequestEntry.copy(it -> it.detail(detail))));
  }

  private Optional<DocumentContextAware<String>> readJsonPathAsStringOptionalFrom(
      final DocumentContext documentContext) {
    return Optional.ofNullable(documentContext.read(jsonPathRemove))
        .map(
            data ->
                data instanceof Map || data instanceof List
                    ? JsonPath.parse(data).jsonString()
                    : data.toString())
        .map(data -> new DocumentContextAware<>(documentContext, data));
  }

  private Either<EventBridgeResult.Error, DocumentContextAware<String>> putS3(
      final DocumentContextAware<String> payload) {

    logger.debug("Found JSON at '{}'.", jsonPathRemove.getPath());

    //  TODO check if payload.value has already S3 (key) object
    var key = idGenerator.get().toString();

    var request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
    try {
      var requestBody = fromString(payload.value, UTF_8);
      client.putObject(request, requestBody).get(SDK_TIMEOUT, MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      var cause = e.getCause();
      if (cause instanceof S3Exception && ((S3Exception) cause).statusCode() < 500) {
        return Either.left(panic(e));
      }
      return Either.left(retry(e));
    } catch (Exception e) {
      return Either.left(panic(e));
    }

    return Either.right(payload.documentContextWith(key));
  }
}
