package com.pabu5h.evs2.evs2helper.alarm;

import com.pabu5h.evs2.evs2helper.email.EmailService;
import com.pabu5h.evs2.evs2helper.locale.LocalHelper;
import com.pabu5h.evs2.oqghelper.OqgHelper;
import com.xt.utils.MathUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

@Component
public class AlarmHelper {
    Logger logger = Logger.getLogger(AlarmHelper.class.getName());

    @Value("${alarm.ack.url.evs2}")
    private String alarmAckUrlEvs2;
    @Value("${alarm.ack.url.smrt}")
    private String alarmAckUrlSmrt;
    @Value("${alarm.ack.url.cwnus}")
    private String alarmAckUrlCwnus;
    @Autowired
    private EmailService emailService;
    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private LocalHelper localHelper;

    public void postAlarmStream(String name, String scopeStr, String siteTag, String message) {
        Map<String, Object> topic = getAlarmTopic(null, name, scopeStr, siteTag);
        if(topic.containsKey("error")) {
            logger.warning("Error getting alarm_topic: " + topic.get("error"));
            return;
        }

        LocalDateTime localNow = localHelper.getLocalNow();
        UUID uuid = UUID.randomUUID();
        String sql = "insert into alarm_stream (alarm_topic_id, message, alarm_timestamp, uid) values (" +
                topic.get("id") + ", '" + message + "', '" + localNow + "' , '" + uuid + "')";
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.warning("Error inserting into alarm_stream: " + e.getMessage());
        }
    }

    public Map<String, Object> sendAlarms(boolean testMode){
        //get all distinct alarm_topic_id from alarm_stream in the last 55 minutes
        LocalDateTime localNow = localHelper.getLocalNow();
        LocalDateTime localNowMinus55 = localNow.minusMinutes(55);
        String sql = "select distinct alarm_topic_id from alarm_stream where alarm_timestamp > '" + localNowMinus55 + "'";

        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_stream: " + e.getMessage());
            return Map.of("error", "Error querying alarm_stream: " + e.getMessage());
        }

        long noticeCount = 0;
        for(Map<String, Object> alarmTopic : resp) {
            long topicId = MathUtil.ObjToLong(alarmTopic.get("alarm_topic_id"));
            Map<String, Object> topic = getAlarmTopic(topicId, null, null, null);
            if(topic.containsKey("error")) {
                logger.warning("Error getting alarm_topic: " + topic.get("error"));
                continue;
            }
            Map<String, Object> sub = getAlarmSubs(topicId);
            if(sub.containsKey("error")) {
                logger.warning("Error getting alarm_sub: " + sub.get("error"));
                continue;
            }

            // get last alarm_stream item for this topic_id
            Map<String, Object> lastAlarmStreamItem = getLastAlarmStreamItem(topicId);
            if(lastAlarmStreamItem.containsKey("error")) {
                logger.warning("Error getting last alarm_stream alarmTopic: " + lastAlarmStreamItem.get("error"));
                continue;
            }
            String lastAlarmTimestampStr = (String) lastAlarmStreamItem.get("alarm_timestamp");
            String lastAlarmMessage = (String) lastAlarmStreamItem.get("message");
            String lastAlarmStreamUid = (String) lastAlarmStreamItem.get("uid");

            List<Map<String, Object>> subs = (List<Map<String, Object>>) sub.get("subs");
            for(Map<String, Object> subItem : subs) {
                Long subId = MathUtil.ObjToLong(subItem.get("id"));
                if(resolveSend(subId, topicId, lastAlarmStreamUid, null)) {
                    String salutation = (String) subItem.get("sub_salutation");
                    String email = (String) subItem.get("sub_email");
                    if(email == null || email.isBlank()){
                        String userTableName = "evs2_user";
                        Long userId = MathUtil.ObjToLong(subItem.get("user_id"));
                        sql = "select email from " + userTableName + " where id = " + userId.toString();
                        List<Map<String, Object>> resp2;
                        try {
                            resp2 = oqgHelper.OqgR2(sql,true);
                        } catch (Exception e) {
                            logger.severe("Failed to query user table: " + e.getMessage());
                            continue;
                        }
                        if(resp2.isEmpty()){
                            logger.info("User not found for user id: " + userId);
                            continue;
                        }
                        email = (String) resp2.getFirst().get("email");
                    }

                    String scopeStr = topic.get("scope_str")==null?"":(String) topic.get("scope_str");
                    String siteTag = topic.get("site_tag")==null?"":(String) topic.get("site_tag");
                    String scopePrefix = scopeStr.isEmpty()?"":scopeStr;
                    scopePrefix += siteTag.isEmpty()?"":"-"+siteTag;
                    String subject = scopePrefix.toUpperCase() + " Alert - " + topic.get("label");

                    String ackUrl = "";
                    if(scopeStr.toLowerCase().contains("ems_smrt")) {
                        ackUrl = alarmAckUrlSmrt;
                    }else if(scopeStr.toLowerCase().contains("ems_cw_nus")) {
                        ackUrl = alarmAckUrlCwnus;
                    }else {
                        ackUrl = alarmAckUrlEvs2;
                    }
                    if(!testMode) {
                        sendAlarmNotice(subId, salutation, email, subject, lastAlarmMessage, lastAlarmStreamUid, ackUrl);
                    }
                    noticeCount++;
                }
            }
        }
        return Map.of("notice_count", noticeCount);
    }
    public void sendAlarmNotice(long subId, String salutation, String email, String subject, String message, String alarmStreamUid, String ackUrl) {
        String fromAddress = "no-reply@evs.com.sg";
        String senderName = "EMS/EVS System Watch"; //evs2OpsName;
        String pleaseAck = "Please click the link below to acknowledge:<br>";
        String ackExplain = "* Acknowledgment will pause the system from sending you further notifications of this alarm topic for the next 24 hours.<br><br>";

        String content = "Hi [[name]],<br><br>"
                + "The following alarm has been triggered that requires your attention:<br><br>"
                + "<span style='color: #D32F2F; font-size: 15px; font-weight: bold; font-family: Arial, sans-serif;'>" + message + "</span><br><br>"
                + pleaseAck
                + "<a href=\"[[URL]]\" style=\"background-color: #00abb5; color: white; padding: 8px 20px; text-align: center; text-decoration: none; display: inline-block; font-size: 16px; margin: 4px 2px; cursor: pointer; border: none; border-radius: 3px;\" target=\"_self\">Acknowledge</a><br>"
                + ackExplain
                + "Thank you,<br>"
                + "EMS/EVS System Watch<br><br>";

        content = content.replace("[[name]]", salutation);

        String ackURL = ackUrl + "/" + subId + "/" + alarmStreamUid;

        content = content.replace("[[URL]]", ackURL);
        try {
            emailService.sendMimeEmail(fromAddress, senderName, email, subject, content, true);
        }catch (Exception e) {
            logger.info("send email error: " + e.getMessage());
        }
    }

    public Map<String, Object> ackAlarm(long subId, String alarmStreamUid, String ackMessage) {
        LocalDateTime localNow = localHelper.getLocalNow();
        String sql = "insert into alarm_ack (alarm_sub_id, alarm_stream_uid, ack_timestamp, ack_message) values (" +
                subId + ", '" + alarmStreamUid + "', '" + localNow + "' , '" + ackMessage + "')";
        try {
            oqgHelper.OqgIU(sql);
            return Map.of("success", "Alarm acknowledged");
        } catch (Exception e) {
            logger.warning("Error updating alarm_ack: " + e.getMessage());
            return Map.of("error", "Error updating alarm_ack: " + e.getMessage());
        }
    }

    public Map<String, Object> isAcked(long subId, String alarmStreamUid) {
        String sql = "select * from alarm_ack where alarm_sub_id = " + subId + " and alarm_stream_uid = '" + alarmStreamUid + "'";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_ack: " + e.getMessage());
            return Map.of("error", "Error querying alarm_ack: " + e.getMessage());
        }
        if(resp.isEmpty()) {
            return Map.of("acked", false);
        }
        return Map.of("acked", true);
    }

    public Map<String, Object> checkAck(long subId, String alarmStreamUid) {
        String sqlStream = "select * from alarm_stream where uid = '" + alarmStreamUid + "'";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sqlStream, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_ack: " + e.getMessage());
            return Map.of("error", "Error querying alarm_ack: " + e.getMessage());
        }
        if(resp.isEmpty()) {
            return Map.of("error", "No alarm_stream found for uid: " + alarmStreamUid);
        }
        String message = (String) resp.getFirst().get("message");

        String sqlAck = "select * from alarm_ack where alarm_sub_id = " + subId + " and alarm_stream_uid = '" + alarmStreamUid + "'";
        List<Map<String, Object>> respAck;
        try {
            respAck = oqgHelper.OqgR2(sqlAck, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_ack: " + e.getMessage());
            return Map.of("error", "Error querying alarm_ack: " + e.getMessage());
        }
        if(respAck.isEmpty()) {
            return Map.of("acked", false, "message", message);
        }
        return Map.of("acked", true, "message", message);
    }

    private boolean resolveSend(long subId, long alarmTopicId, String alarmStreamUid, Duration lookbackDuration){
        if(lookbackDuration == null) {
            lookbackDuration = Duration.ofHours(24);
        }
        LocalDateTime localNow = localHelper.getLocalNow();
        LocalDateTime localNowMinusLookBack = localNow.minus(lookbackDuration);

        // the sub has acknowledged an item in alarm_stream
        // that belongs to this alarm_topic
        // in the last 24 hours
        String sql = "select * from alarm_ack " +
                " inner join alarm_stream on alarm_stream.uid = alarm_ack.alarm_stream_uid " +
                " where " +
                " ack_timestamp is not null " +
                " and alarm_ack.alarm_sub_id = " + subId +
                " and alarm_ack.ack_timestamp > '" + localNowMinusLookBack + "'" +
                " and alarm_stream.alarm_topic_id = " + alarmTopicId;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_ack: " + e.getMessage());
            return false;
        }
        return resp.isEmpty();
    }
    public Map<String, Object> getAlarms(String scopeStr, String siteTag){
        String sql = "select * from alarm_topic where 1 = 1 ";
        if (scopeStr != null && !scopeStr.isEmpty()) {
            sql += " and scope_str ilike '%" + scopeStr + "%' ";
        }
        if (siteTag != null && !siteTag.isEmpty()) {
            sql += " and site_tag = '" + siteTag + "' ";
        }
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_topic: " + e.getMessage());
            return Map.of("error", "Error querying alarm_topic: " + e.getMessage());
        }
        if(resp.isEmpty()) {
            logger.warning("No alarm_topic found for " + scopeStr + " " + siteTag);
            return Map.of("error", "No alarm_topic found for " + scopeStr + " " + siteTag);
        }
        List<Map<String, Object>> alarmList = new ArrayList<>();
        for (Map<String, Object> alarmTopic : resp) {
            long topicId = MathUtil.ObjToLong(alarmTopic.get("id"));
            Map<String, Object> lastAlarmStreamItem = getLastAlarmStreamItem(topicId);
            if(lastAlarmStreamItem.containsKey("error")) {
                logger.warning("Error getting last alarm_stream alarmTopic: " + lastAlarmStreamItem.get("error"));
                continue;
            }
            alarmTopic.put("last_alarm_timestamp", lastAlarmStreamItem.get("alarm_timestamp"));
            alarmTopic.put("last_alarm_message", lastAlarmStreamItem.get("message"));
//            alarmTopic.put("last_alarm_stream_uid", lastAlarmStreamItem.get("uid"));
            alarmList.add(alarmTopic);
        }
        return Map.of("alarm_list", alarmList);
    }
    private Map<String, Object> getLastAlarmStreamItem(long topicId){
        String sql = "select * from alarm_stream where alarm_topic_id = " + topicId + " order by id desc limit 1";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_stream: " + e.getMessage());
            return Map.of("error", "Error querying alarm_stream: " + e.getMessage());
        }
        if(resp.isEmpty()) {
            logger.warning("No alarm_stream found for alarm_topic_id: " + topicId);
            return Map.of("error", "No alarm_stream found for alarm_topic_id: " + topicId);
        }
        return resp.getFirst();
    }

    private Map<String, Object> getAlarmTopic(Long id, String name, String scopeStr, String siteTag){
        String sql = "";
        if(id!=null) {
            sql = "select * from alarm_topic where id = " + id;
        }else {
            sql = "select * from alarm_topic where name = '" + name + "' ";
            if (scopeStr != null && !scopeStr.isEmpty()) {
                sql += " and scope_str ilike '%" + scopeStr + "%' ";
            }
            if (siteTag != null && !siteTag.isEmpty()) {
                sql += " and site_tag = '" + siteTag + "' ";
            }
        }
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_topic: " + e.getMessage());
            return Map.of("error", "Error querying alarm_topic: " + e.getMessage());
        }
        if(resp.isEmpty()) {
            logger.warning("No alarm_topic found for " + name + " " + scopeStr + " " + siteTag);
            return Map.of("error", "No alarm_topic found for " + name + " " + scopeStr + " " + siteTag);
        }
        return resp.getFirst();
    }

    private Map<String, Object> getAlarmSubs(long topicId){
        String sql = "select * from alarm_sub where alarm_topic_id = " + topicId;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(sql, true);
        } catch (Exception e) {
            logger.warning("Error querying alarm_sub: " + e.getMessage());
            return Map.of("error", "Error querying alarm_sub: " + e.getMessage());
        }
        if(resp.isEmpty()) {
            logger.warning("No alarm_sub found for alarm_topic_id: " + topicId);
            return Map.of("error", "No alarm_sub found for alarm_topic_id: " + topicId);
        }
        return Map.of("subs", resp);
    }
}
