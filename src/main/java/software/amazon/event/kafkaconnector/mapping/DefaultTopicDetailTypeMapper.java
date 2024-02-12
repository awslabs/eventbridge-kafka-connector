package software.amazon.event.kafkaconnector.mapping;

import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.AWS_DETAIL_TYPES_DEFAULT;

import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class DefaultTopicDetailTypeMapper implements TopicDetailTypeMapper {

  private EventBridgeSinkConfig eventBridgeSinkConfig;

  @Override
  public String getDetailType(String topic) {
    var detailType = eventBridgeSinkConfig.detailType;
    if (detailType != null) return detailType.replace("${topic}", topic);
    return eventBridgeSinkConfig.detailTypeByTopic.getOrDefault(
        topic, AWS_DETAIL_TYPES_DEFAULT.replace("${topic}", topic));
  }

  @Override
  public void configure(EventBridgeSinkConfig eventBridgeSinkConfig) {
    this.eventBridgeSinkConfig = eventBridgeSinkConfig;
  }
}
