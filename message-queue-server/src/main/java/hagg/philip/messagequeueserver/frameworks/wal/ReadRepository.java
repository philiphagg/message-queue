package hagg.philip.messagequeueserver.frameworks.wal;

import hagg.philip.messagequeueserver.entity.QueueEntity;

public interface ReadRepository {
    QueueEntity read(String topic);
}
