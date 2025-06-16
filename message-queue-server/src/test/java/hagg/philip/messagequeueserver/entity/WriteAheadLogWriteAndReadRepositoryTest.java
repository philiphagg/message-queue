package hagg.philip.messagequeueserver.entity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class WriteAheadLogWriteAndReadRepositoryTest {

    @Mock
    private ApplicationEventPublisher mockApplicationEventPublisher;

    private WriteAheadLogWriteAndReadRepository repository;

    @TempDir
    Path tempDir;

    private QueueEntity testEntity;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        repository = new WriteAheadLogWriteAndReadRepository(mockApplicationEventPublisher);

        Field filestoreField = WriteAheadLogWriteAndReadRepository.class.getDeclaredField("FILESTORE");
        filestoreField.setAccessible(true);
        filestoreField.set(repository, tempDir);

        testEntity = new QueueEntity(
            "test-key".getBytes(StandardCharsets.UTF_8),
            "test-value".getBytes(StandardCharsets.UTF_8),
            "test-topic",
            0,
            1L,
            Instant.now()
        );
    }

    @AfterEach
    void tearDown() {
        // The @TempDir annotation handles cleanup of the directory and its contents.
    }

    @Test
    void testWriteAndRead_SuccessfulCase() throws IOException {
        // Arrange
        String topic = testEntity.topic();
        Path topicPath = tempDir.resolve(topic);
        Files.createDirectories(topicPath);
        Path segmentFile = topicPath.resolve("00000000000000000001.log");
        Files.createFile(segmentFile);

        // Act
        repository.write(testEntity);
        QueueEntity readEntity = repository.read(topic);

        // Assert
        assertNotNull(readEntity);
        assertEquals(testEntity.topic(), readEntity.topic());
        assertEquals(testEntity.partition(), readEntity.partition());
        assertEquals(testEntity.offset(), readEntity.offset());
        // Timestamps can lose nanosecond precision when converted to epoch millis, so compare them this way.
        assertEquals(testEntity.timestamp().toEpochMilli(), readEntity.timestamp().toEpochMilli());
        assertArrayEquals(testEntity.key(), readEntity.key());
        assertArrayEquals(testEntity.value(), readEntity.value());
    }

    @Test
    void testRead_TopicDirectoryDoesNotExist() {
        // Arrange
        String nonExistentTopic = "no-such-topic";

        // Act
        QueueEntity result = repository.read(nonExistentTopic);

        // Assert
        assertNull(result);
    }

    @Test
    void testRead_FileIsEmpty() throws IOException {
        // Arrange
        String topic = "empty-topic";
        Path topicPath = tempDir.resolve(topic);
        Files.createDirectories(topicPath);
        Files.createFile(topicPath.resolve("00000000000000000001.log")); // Create an empty file

        // Act
        QueueEntity result = repository.read(topic);

        // Assert
        assertNull(result);
    }

    @Test
    void testWrite_NoWritableFile_ShouldPublishRetryEvent() {
        // Arrange
        // The topic directory exists, but it contains no files to write to.
        Path topicPath = tempDir.resolve(testEntity.topic());
        topicPath.toFile().mkdirs();

        // Act
        repository.write(testEntity);

        // Assert
        // Verify that the fallback mechanism was triggered.
        ArgumentCaptor<QueueEntity> eventCaptor = ArgumentCaptor.forClass(QueueEntity.class);
        verify(mockApplicationEventPublisher, times(1)).publishEvent(eventCaptor.capture());
        assertEquals(testEntity, eventCaptor.getValue());
    }

    @Test
    void testWrite_CreatesDirectoryIfNotExists() {
        // Arrange
        // The directory for the topic does not exist yet.
        Path segmentFile = tempDir.resolve(testEntity.topic()).resolve("00000000000000000001.log");

        // Act
        // This should fail to write and trigger the retry, but the key is to test directory creation.
        repository.write(testEntity);

        // Assert that the directory was created by the write method.
        assertTrue(Files.exists(tempDir.resolve(testEntity.topic())));

        // Since no file was created, it should have triggered the retry logic.
        verify(mockApplicationEventPublisher, times(1)).publishEvent(testEntity);

    }
}
