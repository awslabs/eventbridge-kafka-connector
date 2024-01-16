/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;

/** The StatusReporter is a scheduled task that logs the throughput of the connector */
public class StatusReporter {

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final Logger log = ContextAwareLoggerFactory.getLogger(StatusReporter.class);
  private final AtomicInteger totalRecordsSent = new AtomicInteger(0);

  private final long interval;
  private final TimeUnit timeUnit;

  public StatusReporter(long interval, TimeUnit timeUnit) {
    this.interval = interval;
    this.timeUnit = timeUnit;
  }

  public void setSentRecords(Integer sentRecords) {
    this.totalRecordsSent.addAndGet(sentRecords);
  }

  public boolean isRunning() {
    return !scheduler.isTerminated();
  }

  public void startAsync() {
    log.info("Starting status reporter");
    scheduler.scheduleAtFixedRate(
        () -> log.info("Total records sent={}", totalRecordsSent.get()),
        interval,
        interval,
        timeUnit);
  }

  public void stopAsync() {
    log.info("Stopping status reporter");
    scheduler.shutdown();
  }
}
