package org.pabuff.evs2helper.locale;

import  org.pabuff.utils.DateTimeUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class LocalSettingSG implements LocalSetting {
    @Override
    public String getCountryCode() {
        return "SG";
    }
    @Override
    public String getTimeZone() {
        return "Asia/Singapore";
    }
    @Override
    public String getTimeZoneOffset() {
        return "+08:00";
    }
    @Override
    public String getCurrency() {
        return "SGD";
    }
    @Override
    public String getLanguage() {
        return "en";
    }
    @Override
    public double getGST() {
        return 9;
    }
    @Override
    public LocalDateTime getLocalNow() {
        return LocalDateTime.now(ZoneId.of(getTimeZone()));
    }
    @Override
    public String getLocalNowStr() {
        //bug: getLocalNow() is already zoned,
//        return DateTimeUtil.getZonedDateTimeStr(getLocalNow(), ZoneId.of(getTimeZone()));
        return DateTimeUtil.getZonedDateTimeStr(LocalDateTime.now(), ZoneId.of("Asia/Singapore"));
    }
}
