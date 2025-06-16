package hagg.philip.messagequeueserver.infrastructure.producer;

public record ProducerMessage(
    String topic,
    String message
) {
}
