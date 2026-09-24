package com.gorkemuysal.notificationservice.notification;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BirthdayEventConsumer {

    private final EmailNotificationService emailNotificationService;

    @KafkaListener(topics = "birthday.upcoming", groupId = "notification-service")
    public void handle(UpcomingBirthdayEvent event) {
        log.info("Event received: Person {}'s  birthday is in {} days({})",
                event.fullName(), event.daysUntil(), event.birthDate());

        emailNotificationService.sendBirthdayReminder(event);
    }
}
