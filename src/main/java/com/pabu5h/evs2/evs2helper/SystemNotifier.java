package com.pabu5h.evs2.evs2helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SystemNotifier {
    @Autowired
    private EmailService emailService;
    @Value("${system.notifier.email.from}")
    private String emailFrom;
    @Value("${system.notifier.email.to}")
    private String emailTo;

    public void sendEmail(String subject, String text) {
        emailService.sendSimpleEmail(emailFrom, emailTo, subject, text);
    }
    public void sendException(String subject, String source, String errorMessage) {
        String text = "Source: " + source + "\n" + "Error Message: " + errorMessage;
        emailService.sendSimpleEmail(emailFrom, emailTo, subject, text);
    }
    public void sendNotice(String subject, String source, Map<String, String> message) {
        String title = message.get("title") == null ? "Message" : message.get("title");
        String text = "Source: " + source + "\n" + title+": " + message.get("message");
        emailService.sendSimpleEmail(emailFrom, emailTo, subject, text);
    }
}
