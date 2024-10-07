package org.pabuff.evs2helper.email;

import jakarta.mail.internet.InternetAddress;
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
        String[] toList = to.split(",");
        message.setTo(toList);
//        message.setTo(to);
        message.setSubject(subject);
        message.setText(text);
        try {
            mailSender.send(message);
        }catch (Exception e){
            logger.info("mailSender error: " + e.getMessage());
        }
    }
    // " to " can be a list of email addresses separated by comma
    // e.g. "email1@domain, email2@domain"
    public void sendMimeEmail(String fromAddress, String senderName, String to, String replayTo,
                              String subject, String text,
                              boolean isHtml) {
        MimeMessage mimeMessage = mailSender.createMimeMessage();
        MimeMessageHelper helper = new MimeMessageHelper(mimeMessage);
        try {
            helper.setFrom(senderName+ " <"+fromAddress+">");
            helper.setTo(InternetAddress.parse(to));
            helper.setReplyTo(replayTo);
            helper.setSubject(subject);
            helper.setText(text, isHtml);

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
        String disclaimer="";
        if(subject.contains("[FH-PA-ALL]")){
            disclaimer = "<br><br><span style='font-size: 10px;'>" +
                    "<span style='background-color: yellow; font-size: 12px; font-style: italic;'>Disclaimer</span>: This email and any files transmitted with it contain confidential information intended solely for the named recipient. " +
                    "The information is privileged and must not be disclosed, shared, copied, or distributed to any party outside the intended recipients. " +
                    "Any use of this email’s content for purposes other than those authorized by the sender is strictly prohibited. " +
                    "If you have received this email in error, please notify the sender immediately and delete this email from your system. " +
                    "If you are not the intended recipient, do not disclose, copy, or rely on the contents of this email, and refrain from sharing it with others. Thank you.</span>";
        }
        try {
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true);
            helper.setFrom(senderName+ " <"+fromAddress+">");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(text+disclaimer, isHtml);

            // Add attachment
            if(attachedFile != null) {
                helper.addAttachment(attachedFile.getName(), attachedFile);
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
