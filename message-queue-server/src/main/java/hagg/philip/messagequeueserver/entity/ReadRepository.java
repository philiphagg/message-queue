package hagg.philip.messagequeueserver.entity;

public interface ReadRepository {
    QueueEntity read(String topic);
}
