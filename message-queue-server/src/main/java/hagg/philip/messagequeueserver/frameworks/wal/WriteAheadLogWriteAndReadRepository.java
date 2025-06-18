package hagg.philip.messagequeueserver.frameworks.wal;

import hagg.philip.messagequeueserver.entity.QueueEntity;
import hagg.philip.messagequeueserver.entity.TopicDTO;
import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.hash;

@Component
public class WriteAheadLogWriteAndReadRepository implements WriteAndReadRepository {
    public static final long ARBITRARY_MESSAGE_SIZE = 1024L;
    private final ApplicationEventPublisher applicationEventPublisher;

    private Path filestore = Path.of("src", "main", "java", "hagg", "philip", "messagequeueserver", "filestore");

    private long MAX_SEGMENT_SIZE = 1024 * 1024;
    private static final String SEGMENT_FILE_EXTENSION = ".log";
    private static final String DELIMITER = "::";
    private static final Base64.Encoder ENCODER = Base64.getEncoder();
    private static final Base64.Decoder DECODER = Base64.getDecoder();

    public WriteAheadLogWriteAndReadRepository(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public QueueEntity read(String topic) {
        Path topicPath = filestore.resolve(topic);
        if (!Files.exists(topicPath)) {
            return null;
        }

        try (Stream<Path> files = Files.list(topicPath)) {
            Optional<Path> oldestSegment = files
                .filter(p -> p.toString().endsWith(SEGMENT_FILE_EXTENSION))
                .min(Comparator.comparing(Path::getFileName));

            if (oldestSegment.isPresent()) {
                try (BufferedReader reader = Files.newBufferedReader(oldestSegment.get())) {
                    String firstLine = reader.readLine();
                    if (firstLine != null && !firstLine.isBlank()) {
                        return deserialize(firstLine);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void write(ProducerMessage message) {
        try {
            int partitionNumber = getPartitionNumber(message);
            String partition = "partition-" + partitionNumber;
            Path topicPath = filestore.resolve(message.topic() ,partition );
            Path activeSegment = getOrCreateActiveSegment(topicPath, ARBITRARY_MESSAGE_SIZE);

            long offset = getNextOffset(activeSegment);

            QueueEntity entity = new QueueEntity(
                message.key(),
                message.message().getBytes(),
                message.topic(),
                partitionNumber,
                offset,
                Instant.now()
            );

            String serializedEntity = serialize(entity);

            Files.write(activeSegment, (serializedEntity + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            System.out.println("Could not append to WAL, sent for retry");
            storeForRetry(message).run();
        }
    }

    private int getPartitionNumber(ProducerMessage message) {
        int defaultPartitions = 5;
        int partitionCount = defaultPartitions;
        Path topicPath = filestore.resolve(message.topic());

        if (Files.exists(topicPath) && Files.isDirectory(topicPath)) {
            try (Stream<Path> stream = Files.list(topicPath)) {
                long count = stream.filter(Files::isDirectory).count();
                if (count > 0) {
                    partitionCount = (int) count;
                }
            } catch (IOException e) {
                System.out.println("Could not list partitions for topic " + message.topic() + ", defaulting to " + defaultPartitions);
            }
        }
        return hash(message.key()) % partitionCount;
    }

    @Override
    public void create(TopicDTO topicDTO) {
        try {
            Path topicPath = filestore.resolve(topicDTO.topic());
            Files.createDirectories(topicPath);

            for (int i = 0; i < topicDTO.partitionCount(); i++) {
                Path partitionPath = topicPath.resolve("partition-" + i);
                Files.createDirectories(partitionPath);
            }
        } catch (IOException e) {
            System.out.println("Directory already exists");
        }
    }

    private long getNextOffset(Path segmentFile) throws IOException {
        List<String> lines = Files.readAllLines(segmentFile);
        if (lines.isEmpty()) {
            return 0L;
        }
        String lastLine = "";
        for (int i = lines.size() - 1; i >= 0; i--) {
            if (!lines.get(i).isBlank()) {
                lastLine = lines.get(i);
                break;
            }
        }
        String[] parts = lastLine.split(DELIMITER, -1);
        return Long.parseLong(parts[4]) + 1L;
    }

    private Path getOrCreateActiveSegment(Path topicPath, long messageSize) throws IOException {
        Optional<Path> latestSegmentOpt = findLatestSegment(topicPath);

        if (latestSegmentOpt.isEmpty()) {
            return createNewSegment(topicPath, 0L);
        }

        Path latestSegment = latestSegmentOpt.get();
        if (Files.size(latestSegment) + messageSize <= MAX_SEGMENT_SIZE) {
            return latestSegment;
        }

        long nextSegmentId = getSegmentId(latestSegment) + 1;
        return createNewSegment(topicPath, nextSegmentId);
    }

    private Path createNewSegment(Path topicPath, long segmentId) throws IOException {
        String segmentName = String.format("%020d" + SEGMENT_FILE_EXTENSION, segmentId);
        Path newSegmentPath = topicPath.resolve(segmentName);
        return Files.createFile(newSegmentPath);
    }

    private Optional<Path> findLatestSegment(Path topicPath) throws IOException {
        if (!Files.exists(topicPath)) {
            return Optional.empty();
        }
        try (Stream<Path> files = Files.list(topicPath)) {
            return files
                .filter(p -> p.toString().endsWith(SEGMENT_FILE_EXTENSION))
                .max(Comparator.comparing(Path::getFileName));
        }
    }

    private long getSegmentId(Path segmentPath) {
        String fileName = segmentPath.getFileName().toString();
        String segmentIdStr = fileName.replace(SEGMENT_FILE_EXTENSION, "");
        return Long.parseLong(segmentIdStr);
    }

    private QueueEntity deserialize(String line) {
        String[] parts = line.split(DELIMITER, -1);
        if (parts.length != 7) {
            throw new IllegalArgumentException("Invalid log entry format. Expected 7 parts, but got " + parts.length);
        }
        byte[] key = parts[0].isEmpty() ? null : DECODER.decode(parts[0]);
        byte[] value = DECODER.decode(parts[1]);
        String topic = parts[2];
        Integer partition = Integer.parseInt(parts[3]);
        long offset = Long.parseLong(parts[4]);
        Instant timestamp = Instant.ofEpochMilli(Long.parseLong(parts[5]));
        return new QueueEntity(key.toString(), value, topic, partition, offset, timestamp);
    }

    private String serialize(QueueEntity entity) {
        String keyBase64 = entity.key() != null ? ENCODER.encodeToString(entity.key()) : "";
        String valueBase64 = ENCODER.encodeToString(entity.value());
        return String.join(DELIMITER,
            keyBase64,
            valueBase64,
            entity.topic(),
            String.valueOf(entity.partition()),
            String.valueOf(entity.offset()),
            String.valueOf(entity.timestamp().toEpochMilli()),
            String.valueOf(entity.size())
        );
    }

    private Runnable storeForRetry(ProducerMessage entity) {
        return () -> {
            System.out.println("Failed to write to segment file for topic: " + entity.topic() + ". Publishing for retry.");
            applicationEventPublisher.publishEvent(entity);
        };
    }
}