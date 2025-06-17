package hagg.philip.messagequeueserver.interfaces.producer;

public record ProducerMessage(
    String key,
    String topic,
    String message
) {
}
