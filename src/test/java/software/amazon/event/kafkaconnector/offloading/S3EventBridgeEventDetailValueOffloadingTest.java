/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.STRICT;
import static software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading.ReplaceWithDataRefJsonTransformer.replaceWithDataRef;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

@ExtendWith(MockitoExtension.class)
class S3EventBridgeEventDetailValueOffloadingTest {

  @Mock private S3Client s3Client;

  @BeforeEach
  void resetMocks() {
    reset(s3Client);
  }

  @AfterEach
  void verifyNoMoreMocksInteractions() {
    verifyNoMoreInteractions(s3Client);
  }

  private final String content =
      "{\n"
          + "    \"detail\": {\n"
          + "        \"value\": {\n"
          + "            \"orderItems\": [\n"
          + "                \"item-1\",\n"
          + "                \"item-2\"\n"
          + "            ],\n"
          + "            \"orderCreatedTime\": \"Wed Dec 27 18:51:39 CET 2023\"\n"
          + "        }\n"
          + "  }\n"
          + "}";

  @Test
  public void testS3Upload() throws JSONException {

    var sut = new S3EventBridgeEventDetailValueOffloading(s3Client, "$.detail.value");
    var sinkRecord = mock(SinkRecord.class);

    var mappedSinkRecord =
        new MappedSinkRecord<>(sinkRecord, PutEventsRequestEntry.builder().detail(content).build());

    var actual = sut.apply(List.of(mappedSinkRecord));

    verify(s3Client)
        .putObject(any(PutObjectRequest.class), any(RequestBody.class)); // TODO verify arguments

    var expected =
        "{\n"
            + "    \"detail\": {\n"
            + "        \"dataref\": \"arn:aws:s3:::bucket_name/key_name\""
            + "  }\n"
            + "}";

    assertEquals(expected, actual.success.get(0).getValue().detail(), STRICT);
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void caseFull() throws JSONException {

    var expected =
        "{\n"
            + "    \"detail\": {\n"
            + "        \"dataref\": \"arn:aws:s3:::bucket_name/key_name\""
            + "  }\n"
            + "}";
    var jsonTransformer = replaceWithDataRef("$.detail.value");

    var captured = new ArrayList<String>();

    var actual = jsonTransformer.apply(content, captured::add, "arn:aws:s3:::bucket_name/key_name");

    assertEquals(expected, actual, STRICT);
    assertEquals(
        "{\n"
            + "            \"orderItems\": [\n"
            + "                \"item-1\",\n"
            + "                \"item-2\"\n"
            + "            ],\n"
            + "            \"orderCreatedTime\": \"Wed Dec 27 18:51:39 CET 2023\"\n"
            + "        }\n",
        captured.get(0),
        STRICT);
  }

  @Test
  public void casePartial() throws JSONException {

    var expected =
        "{\n"
            + "    \"detail\": {\n"
            + "        \"value\": {\n"
            + "            \"orderCreatedTime\": \"Wed Dec 27 18:51:39 CET 2023\"\n"
            + "        },\n"
            + "        \"dataref\": \"arn:aws:s3:::bucket_name/key_name\""
            + "  }\n"
            + "}";
    var jsonTransformer = replaceWithDataRef("$.detail.value.orderItems");

    var captured = new ArrayList<String>();

    var actual = jsonTransformer.apply(content, captured::add, "arn:aws:s3:::bucket_name/key_name");

    assertEquals(expected, actual, STRICT);
    assertEquals("[\"item-1\", \"item-2\" ]", captured.get(0), STRICT);
  }

  @Test
  public void caseNothing() throws JSONException {

    var expected =
        "{\n"
            + "    \"detail\": {\n"
            + "        \"value\": {\n"
            + "            \"orderItems\": [\n"
            + "                \"item-1\",\n"
            + "                \"item-2\"\n"
            + "            ],\n"
            + "            \"orderCreatedTime\": \"Wed Dec 27 18:51:39 CET 2023\"\n"
            + "        }\n"
            + "  }\n"
            + "}";
    var jsonTransformer = replaceWithDataRef("$.detail.value.non-present");

    var captured = new ArrayList<String>();

    var actual = jsonTransformer.apply(content, captured::add, "arn:aws:s3:::bucket_name/key_name");

    assertEquals(expected, actual, STRICT);
    assertThat(captured).isEmpty();
  }
}
