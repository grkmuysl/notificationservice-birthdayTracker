# Notification Service

Bağımsız bir Spring Boot uygulaması. **Birthday Tracker** projesinin `people-service` bileşeninden Kafka üzerinden gelen "yaklaşan doğum günü" event'lerini dinler ve e-posta bildirimi gönderir.

## Mimari

```
people-service (producer)
        │
        │  outbox pattern → Kafka
        ▼
  birthday.upcoming (topic)
        │
        ▼
notification-service (consumer)
        │
        │  retry + DLT
        ▼
   SMTP (MailHog / gerçek sunucu)
```

İki servis arasındaki tek bağlantı Kafka'dır; notification-service, people-service'in veritabanına veya API'sine doğrudan erişmez.

## Sorumluluklar

- `birthday.upcoming` topic'ini dinler.
- Gelen her event için e-posta bildirimi gönderir (`EmailNotificationService`).
- Geçici hatalarda (ör. SMTP sunucusuna erişilemiyor) otomatik retry uygular.
- Tüm denemeler başarısız olursa event'i Dead Letter Topic'e (DLT) taşır.

## Teknoloji Yığını

| Bileşen | Teknoloji |
|---|---|
| Framework | Spring Boot 4.1.1 |
| Mesajlaşma | Apache Kafka (Spring Kafka) |
| E-posta | Spring Mail (SMTP) |
| Retry/DLT | `@RetryableTopic` (Spring Kafka) |
| Test SMTP sunucusu | MailHog |

## Paket Yapısı

```
com.gorkemuysal.notificationservice.notification/
├── UpcomingBirthdayEvent      // Kafka'dan gelen event'in DTO'su
├── BirthdayEventConsumer      // @KafkaListener + @RetryableTopic
└── EmailNotificationService   // JavaMailSender ile e-posta gönderimi
```

## Event Formatı

`birthday.upcoming` topic'ine gelen mesajlar aşağıdaki JSON yapısındadır:

```json
{
  "personId": 5,
  "fullName": "Ada Lovelace",
  "birthDate": "1815-12-10",
  "daysUntil": 3
}
```

## Konfigürasyon (`application.properties`)

```properties
spring.application.name=notificationservice
server.port=8081

# Kafka - Consumer
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=notification-service
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.consumer.properties.spring.json.value.default.type=com.gorkemuysal.notificationservice.notification.UpcomingBirthdayEvent
spring.kafka.consumer.properties.spring.json.trusted.packages=com.gorkemuysal.notificationservice.notification

# Kafka - Producer (RetryableTopic'in retry/DLT topic'lerine yazması için gerekli)
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Mail (MailHog)
spring.mail.host=localhost
spring.mail.port=1025
spring.mail.properties.mail.smtp.auth=false
spring.mail.properties.mail.smtp.starttls.enable=false
```

> **Not:** `spring.kafka.producer.*` ayarı, servisin kendisi normal koşullarda üretici (producer) olarak çalışmasa da zorunludur. `@RetryableTopic`, başarısız mesajları retry/DLT topic'lerine yazarken arka planda bir Kafka producer kullanır; bu ayar olmadan `StringSerializer` varsayılan olarak devreye girer ve `ClassCastException` ile sonsuz retry döngüsüne girilir.

## Retry ve Dead Letter Topic (DLT) Akışı

```java
@RetryableTopic(
    attempts = "4",
    backoff = @Backoff(delay = 2000, multiplier = 2.0),
    dltTopicSuffix = "-dlt",
    include = { Exception.class }
)
@KafkaListener(topics = "birthday.upcoming", groupId = "notification-service")
public void handle(UpcomingBirthdayEvent event) { ... }

@DltHandler
public void handleDlt(UpcomingBirthdayEvent event) { ... }
```

- **4 deneme**, aralarında üstel artan bekleme süresi: 2s → 4s → 8s.
- Oluşan retry topic'leri: `birthday.upcoming-retry-2000`, `-retry-4000`, `-retry-8000`.
- Tüm denemeler tükenirse mesaj `birthday.upcoming-dlt` topic'ine taşınır ve `@DltHandler` metodu bir kez çalışıp durumu loglar.

## Çalıştırma

### Ön koşullar

- Kafka (KRaft modunda, Zookeeper'sız) çalışıyor olmalı — `docker-compose.yml` üzerinden.
- MailHog çalışıyor olmalı (test SMTP sunucusu).

```bash
docker compose up -d kafka kafka-ui mailhog
```

### Uygulamayı başlatma

```bash
./mvnw spring-boot:run
```

Servis `http://localhost:8081` üzerinde ayağa kalkar.

### Kontrol araçları

| Araç | Adres | Amaç |
|---|---|---|
| Kafka UI | http://localhost:8090 | Topic'leri, mesajları ve consumer group'ları izleme |
| MailHog | http://localhost:8025 | Gönderilen test e-postalarını görüntüleme |

## Test Senaryoları

**Mutlu senaryo (uçtan uca):**
1. MailHog açık.
2. `people-service`'in `BirthdayCheckJob`'ı (veya manuel eklenen bir outbox kaydı) event üretir.
3. notification-service event'i alır, e-postayı gönderir.
4. MailHog arayüzünde e-posta görünür.

**Retry/DLT senaryosu:**
1. MailHog kapalı (`docker compose stop mailhog`).
2. Bir event üretilir.
3. Consumer 4 deneme yapar (2s, 4s, 8s aralıklarla), her denemede `MailSendException` alır.
4. Son denemeden sonra event `birthday.upcoming-dlt` topic'ine düşer ve `@DltHandler` loglar.
5. MailHog tekrar açılır (`docker compose up -d mailhog`), yeni üretilen event'ler normal şekilde işlenir.

> **Dikkat:** Test sırasında `people-service`'teki `BirthdayCheckJob` cron ifadesi kısa aralığa (`*/10 * * * * *`) çekilirse, MailHog kapalıyken üretilen event'ler outbox ve Kafka'da birikebilir. Teste başlamadan önce Kafka topic'lerini ve `outbox_events` tablosundaki `PENDING` kayıtları temizlemek, sahte "sonsuz döngü" izlenimini önler.

## Bilinen Sınırlamalar / Gelecek İyileştirmeler

- `BirthdayCheckJob` tarafında henüz idempotency kontrolü yok; aynı kişi için aynı gün birden fazla event üretilebilir. Outbox tablosuna `(person_id, tarih)` bazlı bir unique constraint veya "bugün zaten gönderildi mi" kontrolü eklenmesi planlanıyor.
- Outbox pattern şu an polling (`@Scheduled(fixedDelay = 5000)`) ile çalışıyor. İleride Debezium ile CDC tabanlı bir outbox geçişi değerlendirilebilir.
