package hagg.philip.messagequeueserver.usecase;

import hagg.philip.messagequeueserver.entity.RetryItem;
import hagg.philip.messagequeueserver.frameworks.wal.WriteRepository;
import hagg.philip.messagequeueserver.interfaces.producer.ProducerMessage;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class RetryOrchestrator {

    private static final Duration INITIAL_DELAY = Duration.ofSeconds(5);
    private static final double MULTIPLIER = 2.0;
    private static final int MAX_RETRIES = 5;
    private static final Duration MAX_JITTER = Duration.ofSeconds(1);

    private final Map<String, PriorityQueue<RetryItem>> retryQueues = new ConcurrentHashMap<>();
    private final WriteRepository writeRepository;

    public RetryOrchestrator(WriteRepository writeRepository) {
        this.writeRepository = writeRepository;
    }


    @EventListener
    public void handleRetryableEvent(ProducerMessage message) {
        String queueName = message.topic();
        System.out.printf("Received message for retry queue '%s'. key: %b%n", queueName, message.key());

        Instant nextExecutionTime = Instant.now().plus(INITIAL_DELAY);
        RetryItem retryItem = new RetryItem(message, nextExecutionTime, 1);

        PriorityQueue<RetryItem> queue = retryQueues.computeIfAbsent(queueName, k -> new PriorityQueue<>());
        queue.add(retryItem);
    }


    @Scheduled(fixedRate = 1000)
    public void processRetryQueues() {
        Instant now = Instant.now();

        for (Map.Entry<String, PriorityQueue<RetryItem>> entry : retryQueues.entrySet()) {
            String queueName = entry.getKey();
            PriorityQueue<RetryItem> queue = entry.getValue();

            while (queue.peek() != null && queue.peek().nextExecutionTime().isBefore(now)) {
                RetryItem itemToProcess = queue.poll();

                try {
                    System.out.printf("Retrying message for queue '%s'. key: %b, Attempt: %d%n", queueName, itemToProcess
                        .message()
                        .key(), itemToProcess.retryCount());
                    writeRepository.write(itemToProcess.message());

                } catch (Exception e) {
                    System.out.printf("Retry attempt %d failed for message with key %b.%n",
                        itemToProcess.retryCount(), itemToProcess.message().key());

                    if (itemToProcess.retryCount() < MAX_RETRIES) {
                        int newRetryCount = itemToProcess.retryCount() + 1;
                        Instant newNextExecutionTime = calculateNextExecutionTime(newRetryCount);
                        RetryItem newItem = new RetryItem(itemToProcess.message(), newNextExecutionTime, newRetryCount);
                        queue.add(newItem);
                    } else {
                        System.err.printf("FATAL: Message with key %b has failed all %d retries. Discarding.%n",
                            itemToProcess.message().key(), MAX_RETRIES);
                    }
                }
            }
        }
    }

    private Instant calculateNextExecutionTime(int retryCount) {
        long delayMillis = (long) (INITIAL_DELAY.toMillis() * Math.pow(MULTIPLIER, retryCount - 1));
        long jitterMillis = ThreadLocalRandom.current().nextLong(0, MAX_JITTER.toMillis() + 1);
        return Instant.now().plusMillis(delayMillis + jitterMillis);
    }
}
