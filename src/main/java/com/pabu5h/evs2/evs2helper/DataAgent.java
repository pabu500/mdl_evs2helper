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

    public Map<String, Object> getMeterInfoDtoFromSn(String meterSnStr) {
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
                Map<String, Object> result2 = queryHelper.getMeterInfoDtoFromSn(meterSnStr);
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
    public Map<String, Object> getMeterInfoDtoFromDisplayname(String meterDisplaynameStr) {
        Map<String, Object> result = getMeterSnFromDisplayname(meterDisplaynameStr);
        if(result.containsKey("meter_sn")) {
            String meterSnStr = (String) result.get("meter_sn");
            return getMeterInfoDtoFromSn(meterSnStr);
        }
        return Map.of("info", "meter info not found");
    }

    public Map<String, Object> getMeterDisplaynameFromSn(String meterSnStr) {

        String meterDisplayname = meterInfoCache.getMeterDisplayname(meterSnStr);

        if(meterDisplayname.isEmpty()) {
            Map<String, Object> result = getMeterInfoDtoFromSn(meterSnStr);
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

         if(!meterSn.isEmpty()) {
             return Map.of("meter_sn", meterSn);
         }else {
             try {
                 Map<String, Object> result = owlHelper.getMeterInfoFromMeterDisplayname(meterDisplaynameStr);
                 if (result.containsKey("meter_info")) {
                     MeterInfoDto meterInfoDto = (MeterInfoDto) result.get("meter_info");
                     meterInfoCache.putMeterInfo(meterInfoDto.getMeterSn(), meterInfoDto);
                     meterSn = meterInfoDto.getMeterSn();
                     return Map.of("meter_sn", meterSn);
                 }
             } catch (Exception e) {
                 logger.info("owlHelper error: " + e.getMessage());
                 try {
                    String meterSnStr = queryHelper.getMeterSnFromMeterDisplayname(meterDisplaynameStr);
                    if (!meterSnStr.isEmpty()) {
                        return Map.of("meter_sn", meterSnStr);
                    }
                 } catch (Exception e2) {
                    logger.info("queryHelper error: " + e2.getMessage());
                 }
             }
         }
         return Map.of("info", "meter sn not found");
    }

    public Map<String, Object> getMeterTariffFromSn(String meterSnStr) {
        Map<String, Object> result = getMeterInfoDtoFromSn(meterSnStr);
        if(result.containsKey("meter_info")) {
            MeterInfoDto meterInfoDto = (MeterInfoDto) result.get("meter_info");
            if(meterInfoDto.getTariffPrice()>0.00001) {
                return Map.of("tariff_price", meterInfoDto.getTariffPrice());
            }
        }
        try {
            Map<String, Object> result2 = owlHelper.getMeterInfoFromMeterSn(meterSnStr);
            if (result2.containsKey("meter_info")) {
                MeterInfoDto meterInfoDto = (MeterInfoDto) result2.get("meter_info");
                if(meterInfoDto.getTariffPrice()>0.00001) {
                    return Map.of("tariff_price", meterInfoDto.getTariffPrice());
                }else {
                    throw new Exception("tariff_price is 0");
                }
            }
        } catch (Exception e) {
            logger.info("owlHelper error: " + e.getMessage());

            try {
                Map<String, Object> tariffResult = queryHelper.getMeterTariffFromSn(meterSnStr);
                if (tariffResult.containsKey("tariff_price")) {
                    return tariffResult;
                }
            } catch (Exception e3) {
                logger.info("queryHelper error: " + e3.getMessage());
            }
        }
        return Map.of("info", "meter tariff not found");
    }
    public Map<String, Object> getMeterTariffFromDisplayname(String meterDisplaynameStr) {
        Map<String, Object> result = getMeterSnFromDisplayname(meterDisplaynameStr);
        if(result.containsKey("meter_sn")) {
            String meterSnStr = (String) result.get("meter_sn");
            return getMeterTariffFromSn(meterSnStr);
        }
        return Map.of("info", "meter tariff not found");
    }
}
