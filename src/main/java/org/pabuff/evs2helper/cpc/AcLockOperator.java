package org.pabuff.evs2helper.cpc;

import org.pabuff.dto.SvcClaimDto;
import org.pabuff.evs2helper.device.M3Helper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

@Component
public class AcLockOperator {
    private final Logger logger = Logger.getLogger(AcLockOperator.class.getName());
    final boolean mock = false;
    private final Map<String, String> mockMeterStatus = new ConcurrentHashMap<>();


    @Autowired
    M3Helper m3Helper;

//    public Map<String, Object> turnAcLockOnOff(String meterSn, AcLockTarget acLockTarget, SvcClaimDto svcClaimDto) {
    public Map<String, Object> submitAcLockOnOff(String meterSn, AcLockStatus acLockTarget, SvcClaimDto svcClaimDto) {

        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .operation("turnAcLockOnOff")
                    .endpoint("cpcsvc")
                    .scope("cpcsvc")
                    .svcName("cpcsvc")
                    .target(meterSn)
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
                    result = Map.of("info", "mock: sent RLS "+acLockTarget+" success");
                    mockMeterStatus.put(meterSn, acLockTarget.toString());
                }else{
                    result = Map.of("error", "mock: sent RLS "+acLockTarget+" error");
                }
                return result;
            } catch (InterruptedException e) {
                return Map.of("error", e.getMessage());
            }
        }else {
            String acLockCommand = "";
//        if(acLockTarget == AcLockTarget.on){
            if (acLockTarget == AcLockStatus.on) {
                acLockCommand = "lockon";
//        } else if(acLockTarget == AcLockTarget.off){
            } else if (acLockTarget == AcLockStatus.off) {
                acLockCommand = "lockoff";
            } else {
                return Map.of("error", "Invalid request");
            }
            try {
                result = m3Helper.turnAcLockOnOff(meterSn, acLockCommand, svcClaimDto);
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

    public Map<String, Object> checkAcLockStatus(String meterSn, SvcClaimDto svcClaimDto){
        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .target(meterSn)
                    .operation("checkSvcStatus")
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
                        result = Map.of("result", mockMeterStatus.get(meterSn));
                    }else {
                        result = Map.of("error", "mock: error");
                    }
                }else{
                    //get random number of 0 or 1
                    int random = (int) (Math.random() * 3);
                    if (random == 0) {
                        logger.info("mock: AcLock status is ON");
                        mockMeterStatus.put(meterSn, "ON");
                        result = Map.of("result", "true");
                    } else if (random == 1) {
                        mockMeterStatus.put(meterSn, "OFF");
                        logger.info("mock: RLS status is OFF");
                        result = Map.of("result", "false");
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
                result = m3Helper.checkAcLockStatus(meterSn, 6000, svcClaimDto);
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
}
