package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.MeterInfoDto;
import com.pabu5h.evs2.oqghelper.QueryHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


@Service
public class DataAgent {
    private final Logger logger = Logger.getLogger(DataAgent.class.getName());

    @Autowired
    MeterInfoCache meterInfoCache;
    @Autowired
    OwlHelper owlHelper;
    @Autowired
    QueryHelper queryHelper;

    public Map<String, Object> getMeterInfoDto(String meterSnStr) {
        // 1st try to get from cache,
        // if not found, get from owlHelper,
        // if not found, get from queryHelper
        Map<String, MeterInfoDto> result = meterInfoCache.getMeterInfo(meterSnStr);

        //if (result.containsKey("meter_info")) {
        if (result != null) {
            Map<String, Object> result2 = new HashMap<>();
            result2.put("meter_info", result.get("meter_info"));
            return result2;
        }

        try {
            Map<String, Object> result2 = owlHelper.getMeterInfoFromMeterSn(meterSnStr);
            if (result2.containsKey("meter_info")) {
                result = new HashMap<>();
                result.put("meter_info", (MeterInfoDto) result2.get("meter_info"));
                return result2;
            }
        } catch (Exception e) {
            logger.info("owlHelper error: " + e.getMessage());
            try {
                Map<String, Object> result2 = queryHelper.getMeterInfoDto(meterSnStr);
                if (result2.containsKey("meter_info")) {
                    result = new HashMap<>();
                    result.put("meter_info", (MeterInfoDto) result2.get("meter_info"));
                    return result2;
                }
            } catch (Exception e2) {
                logger.info("queryHelper error: " + e2.getMessage());
            }
        }
        return Map.of("info", "meter info not found");
    }

    public Map<String, Object> getMeterDisplaynameFromSn(String meterSnStr) {

        String meterDisplayname = meterInfoCache.getMeterDisplayname(meterSnStr);

        if(meterDisplayname.isEmpty()) {
            Map<String, Object> result = getMeterInfoDto(meterSnStr);
            if(result.containsKey("meter_info")) {
                MeterInfoDto meterInfoDto = (MeterInfoDto) result.get("meter_info");
                meterInfoCache.putMeterInfo(meterSnStr, meterInfoDto);
                meterDisplayname = meterInfoDto.getMeterDisplayname();
                return Map.of("meter_displayname", meterDisplayname);
            }
        }else {
            return Map.of("meter_displayname", meterDisplayname);
        }
        return Map.of("info", "meter displayname not found");
    }

    public Map<String, Object> getMeterSnFromDisplayname(String meterDisplaynameStr){
        String meterSn = meterInfoCache.getMeterSn(meterDisplaynameStr);

        if(meterSn.isEmpty()) {
            Map<String, Object> result = owlHelper.getMeterInfoFromMeterDisplayname(meterDisplaynameStr);
            if(result.containsKey("meter_info")) {
                MeterInfoDto meterInfoDto = (MeterInfoDto) result.get("meter_info");
                meterInfoCache.putMeterInfo(meterInfoDto.getMeterSn(), meterInfoDto);
                meterSn = meterInfoDto.getMeterSn();
                return Map.of("meter_sn", meterSn);
            }
        }else {
            return Map.of("meter_sn", meterSn);
        }
        return Map.of("info", "meter sn not found");
    }
}
