package com.gorkemuysal.notificationservice.notification;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BirthdayEventConsumer {

    private final EmailNotificationService emailNotificationService;

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 2000, multiplier = 2.0),
            dltTopicSuffix = "-dlt",
            include = { Exception.class }
    )
    @KafkaListener(topics = "birthday.upcoming", groupId = "notification-service")
    public void handle(UpcomingBirthdayEvent event) {
        log.info("Event received: Person {}'s  birthday is in {} days({})",
                event.fullName(), event.daysUntil(), event.birthDate());

        emailNotificationService.sendBirthdayReminder(event);
    }


    @DltHandler
    public void handleDlt(UpcomingBirthdayEvent event) {
        log.error("Entering to DLT, All retries are failed: Person- {}: {}", event, event.fullName());
    }
}
