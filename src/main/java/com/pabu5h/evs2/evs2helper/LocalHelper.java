package com.pabu5h.evs2.evs2helper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class LocalHelper {
    @Value("${local.setting.countryCode}")
    private String countryCode;

    public LocalSetting localSetting() {
        switch (countryCode.toUpperCase()){
            case "SG":
                return new LocalSettingSG();
            default:
                throw new IllegalArgumentException("Invalid country code: " + countryCode);
        }
    }
    public LocalDateTime getLocalNow() {
        return localSetting().getLocalNow();
    }
}
