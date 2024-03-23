/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.lang.String.format;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;

class ReplaceWithDataRefJsonTransformer {

  private static final Logger logger =
      ContextAwareLoggerFactory.getLogger(ReplaceWithDataRefJsonTransformer.class);
  private static final Configuration configuration =
      defaultConfiguration()
          .addOptions(
              SUPPRESS_EXCEPTIONS // suppress exception otherwise
              // com.jayway.jsonpath.ReadContext#read throws an exception if JSON path could not
              // be found
              );
  private static final JsonPath jsonPathAdd = JsonPath.compile("$");

  private final JsonPath jsonPathRemove;
  private final String jsonPathExp;

  private ReplaceWithDataRefJsonTransformer(final String jsonPathExp) {
    JsonPath jsonPath;
    try {
      jsonPath = JsonPath.compile(jsonPathExp);
    } catch (Exception e) {
      throw new IllegalArgumentException(format("Invalid JSON Path '%s'.", jsonPathExp));
    }

    if (!jsonPath.getPath().startsWith("$['detail']['value']")) {
      throw new IllegalArgumentException(
          format("JSON Path must start with '$.detail.value' but is '%s'.", jsonPathExp));
    }
    if (!jsonPath.isDefinite()) {
      throw new IllegalArgumentException(
          format("JSON Path must be definite but '%s' is not.", jsonPathExp));
    }

    // rewrite $.detail -> $, because PutEventsRequestEntry#detail is the sub document of $.detail
    jsonPathRemove = JsonPath.compile("$" + jsonPath.getPath().substring("$['detail']".length()));
    this.jsonPathExp = jsonPathExp;
  }

  public static ReplaceWithDataRefJsonTransformer replaceWithDataRef(String jsonPathExp) {
    return new ReplaceWithDataRefJsonTransformer(jsonPathExp);
  }

  public String apply(final String content, final Function<String, DataRef> onDeleted) {
    var ctx = JsonPath.parse(content, configuration);
    var data = ctx.read(jsonPathRemove);
    if (data != null) {
      logger.debug("Found JSON at '{}'.", jsonPathRemove.getPath());
      DataRef dataRef;
      if (data instanceof Map || data instanceof List) {
        dataRef = onDeleted.apply(JsonPath.parse(data).jsonString());
      } else {
        dataRef = onDeleted.apply(data.toString());
      }
      ctx.delete(jsonPathRemove).put(jsonPathAdd, "dataref", dataRef.toString());
    }
    ctx.put(jsonPathAdd, "datarefPath", jsonPathExp);

    return ctx.jsonString();
  }
}
