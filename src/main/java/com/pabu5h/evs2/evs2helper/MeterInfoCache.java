package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.MeterInfoDto;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter@Setter
public class MeterInfoCache {
    private final Map<String, MeterInfoDto> meterInfoMap = new ConcurrentHashMap<>();

    private volatile boolean refreshing = false;

    public void refreshMeterInfoMap(List<MeterInfoDto> meterInfoList) {
        refreshing = true;
        meterInfoMap.clear();
        for(Object meterInfoObj : meterInfoList) {
            MeterInfoDto meterInfo = MeterInfoDto.fromFieldMap((Map<String, Object>) meterInfoObj);
            meterInfoMap.put(meterInfo.getMeterSn(), meterInfo);
        }
        refreshing = false;
    }
    public boolean isEmpty() {
        return meterInfoMap.isEmpty();
    }

    public String getMeterDisplayname(String meterSn) {
        MeterInfoDto meterInfo = meterInfoMap.get(meterSn);
        if(meterInfo == null) {
            return "";
        }
        return meterInfo.getMeterDisplayname();
    }

    public String getMeterSn(String meterDisplayname){
        for(Map.Entry<String, MeterInfoDto> entry : meterInfoMap.entrySet()){
            if(entry.getValue().getMeterDisplayname().equals(meterDisplayname)){
                return entry.getKey();
            }
        }
        return "";
    }

}
