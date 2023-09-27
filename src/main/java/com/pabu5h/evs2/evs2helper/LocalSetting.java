package com.pabu5h.evs2.evs2helper;

import java.time.LocalDateTime;

public interface LocalSetting {
    String getCountryCode();
    String getTimeZone();
    String getTimeZoneOffset();
    String getCurrency();
    String getLanguage();
    double getGST();
    LocalDateTime getLocalNow();
    String getLocalNowStr();

}
