package hagg.philip.messagequeueserver.frameworks.wal;

import hagg.philip.messagequeueserver.entity.TopicDTO;
import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;

public interface WriteRepository {
    void write(ProducerMessage message);

    void create(TopicDTO topic);
}
