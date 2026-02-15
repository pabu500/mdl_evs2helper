package org.pabuff.evs2helper;

import org.pabuff.dto.MeterBypassDto;
import org.pabuff.dto.MeterInfoDto;
import org.pabuff.evs2helper.cache.MeterInfoCache;
import org.pabuff.oqghelper.QueryHelper;
import org.pabuff.utils.DateTimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class BypassPolicyResolver {
    private final Logger logger = Logger.getLogger(BypassPolicyResolver.class.getName());
    @Autowired
    private MeterInfoCache meterInfoCache;
    @Autowired
    private QueryHelper queryHelper;

    // bypass: {"result": "ok"}
    // no bypass: {"result": "no"}
    public Map<String, Object> resolveBypassPolicy(String meterSn, String timestamp, String bypassPolicyTableName, boolean logging) {
        if(meterSn.equals("202206000050")||
           meterSn.equals("202206000051")||
           meterSn.equals("202206000052")||
           meterSn.equals("202206000053")||
           meterSn.equals("202206000054")||
           meterSn.equals("202206000055")||
           meterSn.equals("202206000056")||
           meterSn.equals("202206000057")){
//            logger.info("debug bypass policy");
        }

        Map<String, MeterInfoDto> meterInfo = meterInfoCache.getMeterInfo(meterSn);
        MeterInfoDto meterInfoDto = null;
        if(meterInfo != null) {
            if(logging){
                logger.info("Meter info found in cache: " + meterSn);
            }
            meterInfoDto = meterInfo.get("meter_info");
        }else {
            Map<String, Object>result = queryHelper.getMeterInfoDtoFromSn2(meterSn, bypassPolicyTableName);
            if(result.containsKey("meter_info")){
                if(logging){
                    logger.info("Meter info found in DB: " + meterSn);
                }
                meterInfoDto = (MeterInfoDto) result.get("meter_info");
            }
        }

        if(meterInfoDto == null) {
            logger.warning("Unable to resolve bypass policy: " + meterSn + ", meter info not found. Default to no bypass.");
            return Map.of("result", "no",
                          "message", "meter info not found");
        }

        // check perm bypass
        // this is the lc status of meter
        // it is different from 'bypass always' from the bypass policy
        if("bypassed".equals(meterInfoDto.getLcStatus())){
            logger.info("Meter lc_status is bypassed: " + meterSn);
            return Map.of("result", "ok",
                          "message", "lc_status is bypassed");
        }

        MeterBypassDto meterBypassDto = meterInfoDto.getBypassPolicy();

        if(meterBypassDto == null) {
            logger.warning("Unable to resolve bypass policy: " + meterSn + ", bypass policy not found. Default to no bypass.");
            return Map.of("result", "no",
                          "message", "bypass policy not found");
        }else{
            if(logging){
                logger.info("Meter bypass policy: " + meterSn + ", " + meterBypassDto.toString());
            }
        }

        if(meterBypassDto.isBypassAlways()){
            return Map.of("result", "ok",
                          "message", "bypass_always");
        }
        try {
            LocalDateTime ts = DateTimeUtil.getLocalDateTime(timestamp);
            LocalDateTime bypass1Start = meterBypassDto.getBypass1StartTimestamp();
            LocalDateTime bypass1End = meterBypassDto.getBypass1EndTimestamp();
            if (bypass1Start != null && bypass1End != null) {
                if (ts.isAfter(bypass1Start) && ts.isBefore(bypass1End)) {
                    return Map.of("result", "ok",
                            "message", "bypass_1");
                }
            }
            LocalDateTime bypass2Start = meterBypassDto.getBypass2StartTimestamp();
            LocalDateTime bypass2End = meterBypassDto.getBypass2EndTimestamp();
            if (bypass2Start != null && bypass2End != null) {
                if (ts.isAfter(bypass2Start) && ts.isBefore(bypass2End)) {
                    return Map.of("result", "ok",
                            "message", "bypass_2");
                }
            }
            LocalDateTime bypass3Start = meterBypassDto.getBypass2StartTimestamp();
            LocalDateTime bypass3End = meterBypassDto.getBypass2EndTimestamp();
            if (bypass3Start != null && bypass3End != null) {
                if (ts.isAfter(bypass3Start) && ts.isBefore(bypass3End)) {
                    return Map.of("result", "ok",
                            "message", "bypass_3");
                }
            }
        }catch (Exception e){
            logger.warning("Error resolving bypass policy for " + meterSn + ": " + e.getMessage() + ". Default to no bypass.");
            return Map.of("result", "no",
                          "message", "error resolving bypass policy");
        }
        return Map.of("result", "no",
                      "message", "not getting ok from bypass policy");
    }

}
