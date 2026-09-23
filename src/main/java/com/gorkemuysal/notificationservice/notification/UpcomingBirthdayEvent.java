package com.gorkemuysal.notificationservice.notification;

import java.time.LocalDate;

public record UpcomingBirthdayEvent(
        Long personId,
        String fullName,
        LocalDate birthDate,
        int daysUntil
) {
}
