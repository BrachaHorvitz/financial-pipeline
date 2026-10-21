package com.finpipeline.integration;

import com.finpipeline.config.RabbitMQConfig;
import com.finpipeline.etl.ETLTransformer;
import com.finpipeline.processor.IdempotentProcessor;
import com.finpipeline.repository.TransactionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for end-to-end tests against real PostgreSQL and RabbitMQ containers.
 *
 * Containers are singletons started once per JVM (not per test class), so every subclass
 * shares one Spring context. This matters for RabbitMQ: two cached contexts would both
 * consume transaction.queue and steal each other's messages. For the same reason, all
 * bean overrides (spies) live here — a spy declared in a subclass would create a second context.
 *
 * Skipped automatically when Docker is not available.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.jpa.hibernate.ddl-auto=create-drop",
                // Keep listener retries short so retry-exhaustion tests stay fast
                "spring.rabbitmq.listener.simple.retry.initial-interval=100ms",
                "spring.rabbitmq.listener.simple.retry.multiplier=1.0"
        })
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractIntegrationTest {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @ServiceConnection
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:3.13-alpine");

    static {
        if (DockerClientFactory.instance().isDockerAvailable()) {
            POSTGRES.start();
            RABBIT.start();
        }
    }

    @Autowired
    protected WebTestClient webTestClient;

    @Autowired
    protected TransactionRepository repository;

    @Autowired
    protected RabbitTemplate rabbitTemplate;

    @Autowired
    protected AmqpAdmin amqpAdmin;

    @Autowired
    protected MeterRegistry meterRegistry;

    // Spies call the real beans unless a test stubs them; Spring resets them after each test.
    @MockitoSpyBean
    protected ETLTransformer etlTransformer;

    @MockitoSpyBean
    protected IdempotentProcessor idempotentProcessor;

    @BeforeEach
    void resetState() {
        amqpAdmin.purgeQueue(RabbitMQConfig.TRANSACTION_QUEUE, false);
        amqpAdmin.purgeQueue(RabbitMQConfig.DEAD_LETTER_QUEUE, false);
        repository.deleteAll();
    }

    protected double counter(String name) {
        return meterRegistry.get(name).counter().count();
    }
}
