package hagg.philip.messagequeueserver.frameworks.wal;

import hagg.philip.messagequeueserver.entity.Message;

public interface ReadRepository {
    Message read(String topic);
}
