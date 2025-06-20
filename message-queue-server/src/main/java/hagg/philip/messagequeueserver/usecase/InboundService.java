package hagg.philip.messagequeueserver.usecase;

import hagg.philip.messagequeueserver.interfaces.type.TopicDTO;
import hagg.philip.messagequeueserver.frameworks.wal.WriteAndReadRepository;
import hagg.philip.messagequeueserver.frameworks.wal.WriteRepository;
import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;
import org.springframework.stereotype.Service;

@Service
public class InboundService implements ProducerService {
    private final WriteRepository writeRepository;

    public InboundService(WriteAndReadRepository writeRepository) {
        this.writeRepository = writeRepository;
    }

    public void save(ProducerMessage message) {
        writeRepository.write(message);
    }

    public void createTopic(String topic, Integer partitionCount) {
        writeRepository.create(new TopicDTO(topic, partitionCount));
    }
}
