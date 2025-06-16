package hagg.philip.messagequeueserver.usecase;

import hagg.philip.messagequeueserver.infrastructure.producer.ProducerMessage;

public interface ProducerService {
    void save(ProducerMessage producerMessage);
}
