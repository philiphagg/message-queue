package hagg.philip.messagequeueserver.entity;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Component
public class WriteAheadLogWriteAndReadRepository implements WriteAndReadRepository {
    private final ApplicationEventPublisher applicationEventPublisher;

    private final Path FILESTORE = Path.of("src", "main", "java", "hagg", "philip", "messagequeueserver", "domain", "filestore");

    private static final String DELIMITER = "::";
    private static final Base64.Encoder ENCODER = Base64.getEncoder();
    private static final Base64.Decoder DECODER = Base64.getDecoder();


    public WriteAheadLogWriteAndReadRepository(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public QueueEntity read(String topic) {
        Path topicPath = FILESTORE.resolve(topic);
        File topicFolder = topicPath.toFile();

        if (!topicFolder.exists() || !topicFolder.isDirectory()) {
            return null;
        }

        File[] files = topicFolder.listFiles();
        AtomicReference<QueueEntity> entityRef = new AtomicReference<>();

        Arrays.stream(Objects.requireNonNull(files))
            .filter(File::isFile)
            .filter(File::canRead)
            .findFirst()
            .ifPresent(file -> {
                try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                    String firstLine = reader.readLine();
                    if (firstLine != null && !firstLine.isBlank()) {
                        entityRef.set(deserialize(firstLine));
                    }
                } catch (IOException e) {
                    System.err.println("Error reading file: " + file.getPath());
                    e.printStackTrace();
                } catch (Exception e) {
                    System.err.println("Failed to parse line: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        return entityRef.get();
    }

    @Override
    public void write(QueueEntity entity) {
        Path topicPath = FILESTORE.resolve(entity.topic());
        File topicFolder = topicPath.toFile();

        if (!topicFolder.exists()) {
            if (!topicFolder.mkdirs()) {
                System.err.println("Could not create directory: " + topicPath);
                storeForRetry(entity).run();
                return;
            }
        }

        File[] files = topicFolder.listFiles();
        Arrays.stream(Objects.requireNonNull(files))
            .filter(File::isFile)
            .filter(File::canWrite)
            .findFirst()
            .ifPresentOrElse(
                save(entity),
                storeForRetry(entity)
            );
    }

    public QueueEntity deserialize(String line) {
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

    public String serialize(QueueEntity entity) {
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
            System.out.println("No writable segment file found for topic: " + entity.topic() + ". Publishing for retry.");
            applicationEventPublisher.publishEvent(entity);
        };
    }

    private Consumer<File> save(QueueEntity entity) {
        return file -> {
            file.setWritable(false);
            try {
                Path path = file.toPath();
                String newRow = serialize(entity);
                Files.write(path, (newRow + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                file.setWritable(true);
            }
        };
    }
}