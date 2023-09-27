package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.evs2helper.LocalSetting;
import com.xt.utils.DateTimeUtil;

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
        return 8;
    }
    @Override
    public LocalDateTime getLocalNow() {
        return LocalDateTime.now(ZoneId.of(getTimeZone()));
    }
    @Override
    public String getLocalNowStr() {
        return DateTimeUtil.getZonedDateTimeStr(getLocalNow(), ZoneId.of(getTimeZone()));
    }
}
