package hagg.philip.messagequeueserver.entity;

import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;

import java.time.Instant;

public record RetryItem(
    ProducerMessage message,
    Instant nextExecutionTime,
    int retryCount
) implements Comparable<RetryItem> {

    @Override
    public int compareTo(RetryItem other) {
        return this.nextExecutionTime.compareTo(other.nextExecutionTime);
    }
}
