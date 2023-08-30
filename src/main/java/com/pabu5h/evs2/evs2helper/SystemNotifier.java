package com.pabu5h.evs2.evs2helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
}
