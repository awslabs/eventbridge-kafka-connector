package software.amazon.event.kafkaconnector.mapping;

import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public interface DetailTypeMapper {
  String getDetailType(SinkRecord topic);

  void configure(EventBridgeSinkConfig eventBridgeSinkConfig);
}
