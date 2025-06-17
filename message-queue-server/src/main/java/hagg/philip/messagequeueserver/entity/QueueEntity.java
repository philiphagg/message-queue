package hagg.philip.messagequeueserver.entity;

import java.time.Instant;

import static hagg.philip.messagequeueserver.util.Util.hash;
import static hagg.philip.messagequeueserver.util.Util.nonNull;

public record QueueEntity(
    byte[] key,
    byte[] value,
    String topic,
    Integer partition,
    long offset,
    Instant timestamp,
    int size
) {
    public QueueEntity(String key, byte[] value, String topic, Integer partition, long offset, Instant timestamp) {
        this(hash(key), nonNull(value), nonNull(topic), nonNull(partition), nonNull(offset), nonNull(timestamp), value.length);
    }
}
