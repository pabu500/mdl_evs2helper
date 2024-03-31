package com.pabu5h.evs2.evs2helper.email;

import jakarta.mail.internet.MimeMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamSource;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;


@Component
public class EmailService {
    private final Logger logger = Logger.getLogger(EmailService.class.getName());
    @Autowired
    private JavaMailSender mailSender;

//    public EmailService(JavaMailSender mailSender) {
//        this.mailSender = mailSender;
//    }
    public void sendSimpleEmail(String from, String to, String subject, String text) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(from);
        message.setTo(to);
        message.setSubject(subject);
        message.setText(text);
        try {
            mailSender.send(message);
        }catch (Exception e){
            logger.info("mailSender error: " + e.getMessage());
        }
    }
    public void sendMimeEmail(String fromAddress, String senderName, String to, String subject, String text, boolean isHtml) {
        MimeMessage mimeMessage = mailSender.createMimeMessage();
        MimeMessageHelper message = new MimeMessageHelper(mimeMessage);
        try {
            message.setFrom(senderName+ " <"+fromAddress+">");
            message.setTo(to);
            message.setSubject(subject);
            message.setText(text, isHtml);

            try {
                mailSender.send(mimeMessage);
            }catch (Exception e){
                logger.info("mailSender error: " + e.getMessage());
            }
        }catch (Exception e){
            logger.info("mailSender error: " + e.getMessage());
        }
    }
    public void sendEmailWithAttachment(String fromAddress, String senderName, String to, String subject, String text, File attachedFile, boolean isHtml) {
        MimeMessage mimeMessage = mailSender.createMimeMessage();
        try {
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
            helper.setFrom(senderName+ " <"+fromAddress+">");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(text, isHtml);

            // Add attachment
            helper.addAttachment(attachedFile.getName(), attachedFile);

            try {
                mailSender.send(mimeMessage);
            }catch (Exception e){
                logger.info("mailSender error: " + e.getMessage());
            }
        }catch (Exception e){
            logger.info("mailSender error: " + e.getMessage());
        }
    }
    public void sendEmailWithAttachmentIss(String fromAddress, String senderName, String to,
                                           String subject, String text,
                                           String attachmentName, InputStreamSource attachedFile,
                                           boolean isHtml) {
        MimeMessage mimeMessage = mailSender.createMimeMessage();
        try {
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
            helper.setFrom(senderName+ " <"+fromAddress+">");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(text, isHtml);

            // Add attachment
            helper.addAttachment(attachmentName, attachedFile);

            try {
                mailSender.send(mimeMessage);
            }catch (Exception e){
                logger.info("mailSender error: " + e.getMessage());
            }
        }catch (Exception e){
            logger.info("mailSender error: " + e.getMessage());
        }
    }
    public void sendEmailWithAttachmentIssMulti(String fromAddress, String senderName, String to, String replayTo,
                                                String subject, String text,
                                                List<Map<String, Object>> files,
                                                boolean isHtml) {
        MimeMessage mimeMessage = mailSender.createMimeMessage();
        try {
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
            helper.setFrom(senderName+ " <"+fromAddress+">");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(text, isHtml);
            helper.setReplyTo(replayTo);

            // Add attachment
            for(Map<String, Object> file : files){
                String attachmentName = (String) file.get("name");
                InputStreamSource attachedFile = (InputStreamSource) file.get("content");
                helper.addAttachment(attachmentName, attachedFile);
            }

            try {
                mailSender.send(mimeMessage);
            }catch (Exception e){
                logger.info("mailSender error: " + e.getMessage());
            }
        }catch (Exception e){
            logger.info("mailSender error: " + e.getMessage());
        }
    }
}
