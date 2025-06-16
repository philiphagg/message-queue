package hagg.philip.messagequeueserver.infrastructure.producer;

import hagg.philip.messagequeueserver.usecase.ProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class ProducerController {
    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("inbound/message")
    public ResponseEntity<String> sendMessage(@RequestBody ProducerMessage message) {
        producerService.save(message);
        return ResponseEntity.ok("Message sent");
    }
}
