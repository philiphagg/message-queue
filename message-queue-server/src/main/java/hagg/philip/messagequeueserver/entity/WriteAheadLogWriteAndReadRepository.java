package hagg.philip.messagequeueserver.entity;

import hagg.philip.messagequeueserver.entity.QueueEntity;
import hagg.philip.messagequeueserver.entity.WriteAndReadRepository;
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
import java.util.Optional;
import java.util.stream.Stream;

@Component
public class WriteAheadLogWriteAndReadRepository implements WriteAndReadRepository {
    private final ApplicationEventPublisher applicationEventPublisher;

    private Path filestore = Path.of("src", "main", "java", "hagg", "philip", "messagequeueserver", "domain", "filestore");

    private long MAX_SEGMENT_SIZE = 1024; // 1 KB
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
    public void write(QueueEntity entity) {
        try {
            Path topicPath = filestore.resolve(entity.topic());
            Files.createDirectories(topicPath); // Ensure directory exists

            String newRow = serialize(entity);
            byte[] newRowBytes = newRow.getBytes();

            Path activeSegment = getOrCreateActiveSegment(topicPath, newRowBytes.length);

            Files.write(activeSegment, (newRow + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            e.printStackTrace();
            storeForRetry(entity).run();
        }
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
        return new QueueEntity(key, value, topic, partition, offset, timestamp);
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

    private Runnable storeForRetry(QueueEntity entity) {
        return () -> {
            System.out.println("Failed to write to segment file for topic: " + entity.topic() + ". Publishing for retry.");
            applicationEventPublisher.publishEvent(entity);
        };
    }
}