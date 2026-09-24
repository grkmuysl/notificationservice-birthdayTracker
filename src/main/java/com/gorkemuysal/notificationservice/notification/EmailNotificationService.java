package com.gorkemuysal.notificationservice.notification;

import lombok.RequiredArgsConstructor;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EmailNotificationService {
    private final JavaMailSender mailSender;

    public void sendBirthdayReminder(UpcomingBirthdayEvent event) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom("no-reply@birthday-tracker.local");
        message.setTo("test@example.com");
        message.setSubject("Upcoming Birthday: " + event.fullName());
        message.setText("""
            Hello,

            Birthday of %s is after  %d days later (%s).

            Have a good day!
            """.formatted(event.fullName(), event.daysUntil(), event.birthDate()));

        mailSender.send(message);
    }

}
