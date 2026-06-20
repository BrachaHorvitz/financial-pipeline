package com.finpipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Configures async execution for the pipeline.
 *
 * @EnableAsync activates Spring's @Async support.
 * Without an explicit Executor bean, Spring creates a new thread per call — wasteful.
 * This config provides a bounded thread pool with named threads for observability.
 */
@Configuration
@EnableAsync
public class ConcurrencyConfig implements AsyncConfigurer {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyConfig.class);

    @Bean(name = "pipelineExecutor")
    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Threads always alive, waiting for work
        executor.setCorePoolSize(4);

        // Maximum threads under heavy load
        executor.setMaxPoolSize(10);

        // Tasks waiting when all threads are busy
        executor.setQueueCapacity(100);

        // Prefix visible in logs and thread dumps — makes debugging easy
        executor.setThreadNamePrefix("pipeline-");

        // Wait for running tasks to finish on shutdown — never lose a message
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);

        executor.initialize();

        log.info("Pipeline executor initialized — core={}, max={}, queue={}",
                executor.getCorePoolSize(),
                executor.getMaxPoolSize(),
                executor.getQueueCapacity());

        return executor;
    }

    /**
     * Called when an @Async method throws an unchecked exception.
     * Without this, exceptions in async methods are silently swallowed.
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (ex, method, params) ->
                log.error("Uncaught exception in async method '{}': {}",
                        method.getName(), ex.getMessage(), ex);
    }
}