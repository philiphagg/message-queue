package hagg.philip.messagequeueserver.entity;

public record TopicDTO(
    String topic,
    Integer partitionCount
) {
    public TopicDTO {
        partitionCount = partitionCount == null ? 5 : partitionCount;
    }
}
