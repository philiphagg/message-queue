package hagg.philip.messagequeueserver.frameworks.wal;

import hagg.philip.messagequeueserver.interfaces.type.TopicDTO;
import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;

public interface WriteRepository {
    void write(ProducerMessage message);

    void create(TopicDTO topic);
}
