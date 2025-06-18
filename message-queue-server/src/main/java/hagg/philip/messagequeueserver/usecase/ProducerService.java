package hagg.philip.messagequeueserver.usecase;

import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;

public interface ProducerService {
    void save(ProducerMessage producerMessage);
    void createTopic(String topic, Integer partitionCount);
}
