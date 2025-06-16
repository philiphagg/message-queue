package hagg.philip.messagequeueserver.entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class WriteAheadLogWriteAndReadRepositoryTest {

    @Mock
    private ApplicationEventPublisher mockApplicationEventPublisher;

    private WriteAheadLogWriteAndReadRepository repository;
    @TempDir
    Path tempDir;

    private final String TOPIC = "fifo-topic";

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        repository = new WriteAheadLogWriteAndReadRepository(mockApplicationEventPublisher);
        setRepositoryInternalState("filestore", tempDir);
    }

    @Test
    void write_shouldCreateFirstSegmentWhenTopicIsNew() throws IOException {
        // Arrange
        QueueEntity entity = createEntity(1L);

        // Act
        repository.write(entity);

        // Assert
        Path topicPath = tempDir.resolve(TOPIC);
        assertTrue(Files.exists(topicPath), "Topic directory should be created.");

        List<Path> segments;
        try (Stream<Path> files = Files.list(topicPath)) {
            segments = files.toList();
        }
        assertEquals(1, segments.size(), "Exactly one segment file should be created.");
        assertEquals("00000000000000000000.log", segments.getFirst().getFileName().toString());

        verify(mockApplicationEventPublisher, never()).publishEvent(any());
    }

    @Test
    void read_shouldReturnOldestMessageForFifoBehavior() throws IOException {
        // Arrange
        QueueEntity firstEntity = createEntity(1L, "first-message");
        QueueEntity secondEntity = createEntity(2L, "second-message");

        // Act
        repository.write(firstEntity);
        repository.write(secondEntity);

        QueueEntity result = repository.read(TOPIC);

        // Assert
        assertNotNull(result, "Read should return an entity.");
        assertEquals(firstEntity.offset(), result.offset(), "Should read the first entity written (FIFO).");
        assertArrayEquals(firstEntity.value(), result.value());
    }

    @Test
    void read_shouldReturnNullWhenTopicDoesNotExist() {
        // Act
        QueueEntity result = repository.read("non-existent-topic");

        // Assert
        assertNull(result);
    }

    @Test
    void write_shouldRollToNewSegmentWhenActiveSegmentIsFull() throws Exception {
        // Arrange
        setRepositoryInternalState("MAX_SEGMENT_SIZE", 100L);

        QueueEntity largeEntity = createEntity(1L, "This is a large message value designed to exceed the small segment size limit.");
        QueueEntity secondEntity = createEntity(2L, "This message goes into the new segment.");

        // Act
        repository.write(largeEntity);
        repository.write(secondEntity);

        // Assert
        Path topicPath = tempDir.resolve(TOPIC);
        List<Path> segments;
        try (Stream<Path> files = Files.list(topicPath)) {
            segments = files.sorted().toList();
        }

        assertEquals(2, segments.size(), "There should be two segment files after rolling.");
        assertEquals("00000000000000000000.log", segments.get(0).getFileName().toString());
        assertEquals("00000000000000000001.log", segments.get(1).getFileName().toString());

        String secondSegmentContent = Files.readString(segments.get(1));
        String encodedExpected = Base64.getEncoder().encodeToString("This message goes into the new segment.".getBytes(StandardCharsets.UTF_8));

        assertTrue(secondSegmentContent.contains(encodedExpected), "The second entity should be in the new segment file.");
    }

    @Test
    void read_shouldStillReturnOldestMessageAfterSegmentRoll() throws Exception {
        // Arrange
        setRepositoryInternalState("MAX_SEGMENT_SIZE", 100L);
        QueueEntity firstEntity = createEntity(1L, "This is the very first message in the first segment.");
        QueueEntity secondEntity = createEntity(2L, "This message will be in the second segment.");

        // Act
        repository.write(firstEntity);
        repository.write(secondEntity);

        QueueEntity result = repository.read(TOPIC);

        // Assert
        assertNotNull(result, "Read should return an entity even after a segment roll.");
        assertEquals(firstEntity.offset(), result.offset(), "Read should return the oldest message, which is in the first segment.");
        assertArrayEquals(firstEntity.value(), result.value());
    }

    private void setRepositoryInternalState(String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field field = WriteAheadLogWriteAndReadRepository.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(repository, value);
    }

    private QueueEntity createEntity(long offset, String value) {
        return new QueueEntity(
            ("key-" + offset).getBytes(StandardCharsets.UTF_8),
            value.getBytes(StandardCharsets.UTF_8),
            TOPIC,
            0,
            offset,
            Instant.now()
        );
    }

    private QueueEntity createEntity(long offset) {
        return createEntity(offset, "default-value");
    }
}
