package hagg.philip.messagequeueserver.usecase;

import hagg.philip.messagequeueserver.entity.QueueEntity;
import hagg.philip.messagequeueserver.entity.WriteRepository;
import hagg.philip.messagequeueserver.entity.WriteAndReadRepository;
import hagg.philip.messagequeueserver.infrastructure.producer.ProducerMessage;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Service
public class InboundService implements ProducerService {
    private final WriteRepository writeAndReadRepository;

    public InboundService(WriteAndReadRepository writeAndReadRepository) {
        this.writeAndReadRepository = writeAndReadRepository;
    }

    public void save(ProducerMessage message){
        writeAndReadRepository.write(new QueueEntity(
            new byte[20],
            message.message().getBytes(StandardCharsets.UTF_8),
            "Topic-1",
            1,
            1L,
            Instant.now()
        ));
    }
}
