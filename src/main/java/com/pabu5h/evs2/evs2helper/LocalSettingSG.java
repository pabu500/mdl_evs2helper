package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.evs2helper.LocalSetting;

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
}
