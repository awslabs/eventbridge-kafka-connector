/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.event.kafkaconnector.util;

import com.google.common.util.concurrent.AbstractScheduledService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The StatusReporter is a scheduled task that logs the throughput of the connector */
public class StatusReporter extends AbstractScheduledService {

  private final Logger log = LoggerFactory.getLogger(StatusReporter.class);
  private AtomicInteger sentRecords = new AtomicInteger(0);
  private AtomicInteger totalRecordsSent = new AtomicInteger(0);

  private final long interval;
  private final TimeUnit timeUnit;

  public StatusReporter(long interval, TimeUnit timeUnit) {
    this.interval = interval;
    this.timeUnit = timeUnit;
  }

  public void setSentRecords(Integer sentRecords) {
    this.sentRecords.set(sentRecords);
    this.totalRecordsSent.addAndGet(sentRecords);
  }

  @Override
  protected void runOneIteration() {
    log.info("Total records sent={}", totalRecordsSent.get());
    sentRecords.set(0);
  }

  @Override
  protected void startUp() {
    log.info("Starting status reporter");
  }

  @Override
  protected void shutDown() {
    log.info("Stopping status reporter");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, interval, timeUnit);
  }
}
