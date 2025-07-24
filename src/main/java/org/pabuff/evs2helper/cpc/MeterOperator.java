package org.pabuff.evs2helper.cpc;

import org.pabuff.dto.SvcClaimDto;
import org.pabuff.evs2helper.device.M3Helper;
import org.pabuff.oqghelper.OqgHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

@Component
public class MeterOperator {
    final boolean mock = false;
    private final Map<String, String> mockMeterStatus = new ConcurrentHashMap<>();

    private final Logger logger = Logger.getLogger(MeterOperator.class.getName());

    @Autowired
    M3Helper m3Helper;
    @Autowired
    OqgHelper oqgHelper;

    public Map<String, Object> turnMeterOnOff(String meterSn, String meterRlsTarget, SvcClaimDto svcClaimDto) {

        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .operation("turnMeterOnOff")
                    .endpoint("cpcsvc")
                    .scope("cpcsvc")
                    .svcName("cpcsvc")
                    .target(meterSn)
                    .username("cpcsvc")
                    .build();
        }
        Map<String, Object> result;
        try {
            result = m3Helper.turnMeterOnOff(meterSn, meterRlsTarget, 3000, svcClaimDto);
            if (result.get("info") != null) {
                return result;
            }
            if (result.get("error") != null) {
                return result;
            }
            return result;
        } catch (Exception e) {
            return Map.of("error", e.getMessage());
        }
    }

    public Map<String, Object> submitRelayOnOff(String meterSn, String meterRlsTarget, SvcClaimDto svcClaimDto) {
        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .target(meterSn)
                    .operation("submitRlsOnOff")
                    .endpoint("cpcsvc")
                    .scope("global")
                    .svcName("cpcsvc")
                    .username("cpcsvc")
                    .build();
        }
        Map<String, Object> result;
        if(mock){
            try {
                //sleep
                Thread.sleep(2000);
                //get random number of 0 or 1
                int random = (int) (Math.random() * 2);
                if(random== 0){
                    result = Map.of("info", "mock: sent RLS "+meterRlsTarget+" success");
                    mockMeterStatus.put(meterSn, meterRlsTarget);
                }else{
                    result = Map.of("error", "mock: sent RLS "+meterRlsTarget+" error");
                }
                return result;
            } catch (InterruptedException e) {
                return Map.of("error", e.getMessage());
            }
        }else {
            try {
                result = m3Helper.submitRelayOnOff(meterSn, meterRlsTarget, svcClaimDto);
                if (result.get("info") != null) {
                    return result;
                }
                if (result.get("error") != null) {
                    return result;
                }
                return result;
            } catch (Exception e) {
                return Map.of("error", e.getMessage());
            }
        }
    }
    public Map<String, Object> checkRlsStatus(String meterSn, SvcClaimDto svcClaimDto){
        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .target(meterSn)
                    .operation("checkRlsStatus")
                    .endpoint("cpcsvc")
                    .scope("global")
                    .svcName("cpcsvc")
                    .username("cpcsvc")
                    .build();
        }
        Map<String, Object> result;
        if(mock){
            try {
                //sleep
                Thread.sleep(3000);
                if(mockMeterStatus.get(meterSn) != null) {
                    int random = (int) (Math.random() * 2);
                    if(random== 0) {
                        result = Map.of("meter_rls", mockMeterStatus.get(meterSn));
                    }else {
                        result = Map.of("error", "mock: error");
                    }
                }else{
                    //get random number of 0 or 1
                    int random = (int) (Math.random() * 3);
                    if (random == 0) {
                        logger.info("mock: RLS status is ON");
                        mockMeterStatus.put(meterSn, "ON");
                        result = Map.of("meter_rls", "ON");
                    } else if (random == 1) {
                        mockMeterStatus.put(meterSn, "OFF");
                        logger.info("mock: RLS status is OFF");
                        result = Map.of("meter_rls", "OFF");
                    }else {
                        result = Map.of("error", "mock: error");
                    }
                }
                return result;
            } catch (InterruptedException e) {
                return Map.of("error", e.getMessage());
            }
        }else {
            try {
                result = m3Helper.getMeterRLS(meterSn, 5000, svcClaimDto);
                if (result.get("info") != null) {
                    return result;
                }
                if (result.get("error") != null) {
                    return result;
                }
                return result;
            } catch (Exception e) {
                return Map.of("error", e.getMessage());
            }
        }
    }

    public Map<String, Object> checkRlsMid(String meterSn, String mid, SvcClaimDto svcClaimDto){
        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .target(meterSn)
                    .operation("checkRlsMid")
                    .endpoint("cpcsvc")
                    .scope("global")
                    .svcName("cpcsvc")
                    .username("cpcsvc")
                    .build();
        }
        Map<String, Object> result;
        try {
            result = m3Helper.checkRlsMidSubscribe(meterSn, mid, svcClaimDto);
            if (result.get("info") != null) {
                return result;
            }
            if (result.get("error") != null) {
                return result;
            }
            return result;
        } catch (Exception e) {
            return Map.of("error", e.getMessage());
        }

    }
}
