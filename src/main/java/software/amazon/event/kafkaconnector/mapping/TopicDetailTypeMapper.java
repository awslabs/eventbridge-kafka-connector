package software.amazon.event.kafkaconnector.mapping;

import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public interface TopicDetailTypeMapper {
  String getDetailType(String topic);

  void configure(EventBridgeSinkConfig eventBridgeSinkConfig);
}
