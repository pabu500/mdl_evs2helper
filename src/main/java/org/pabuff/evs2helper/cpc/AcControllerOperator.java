package org.pabuff.evs2helper.cpc;

import org.pabuff.dto.QueryCredDto;
import org.pabuff.dto.QueryReqDto;
import org.pabuff.dto.StdRespDto;
import org.pabuff.dto.SvcClaimDto;
import org.pabuff.evs2helper.mqtt.PagMqttAgentHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

@Component
public class AcControllerOperator {
    private final Logger logger = Logger.getLogger(AcControllerOperator.class.getName());

    final boolean mock = false;
    private final Map<String, String> mockMeterStatus = new ConcurrentHashMap<>();

    @Autowired
    PagMqttAgentHelper pagMqttAgentHelper;
    @Autowired
    GatewayResolver gatewayResolver;

    public Map<String, Object> submitServiceOnOff(String meterSn, AcControllerStatus desiredAcControllerStatus, SvcClaimDto svcClaimDto) {

        if(svcClaimDto == null) {
            svcClaimDto = SvcClaimDto.builder()
                    .operation("turnAcOnOff")
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
                    result = Map.of("info", "mock: sent RLS "+desiredAcControllerStatus+" success");
                    mockMeterStatus.put(meterSn, desiredAcControllerStatus.toString());
                }else{
                    result = Map.of("error", "mock: sent RLS "+desiredAcControllerStatus+" error");
                }
                return result;
            } catch (InterruptedException e) {
                return Map.of("error", e.getMessage());
            }
        }else {
            String acControllerCommand = "";
            if (desiredAcControllerStatus == AcControllerStatus.service_on) {
                acControllerCommand = "turn_service_on";
            } else if (desiredAcControllerStatus == AcControllerStatus.service_off) {
                acControllerCommand = "turn_service_off";
            } else {
                return Map.of("error", "Invalid request");
            }
            try {
                QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");

                //resolve gateway
                String gw = null;
                Map<String, Object> scope = Map.of(
                        "project_name", "evs2_nus",
                        "site_tag", "vh"
                );
                Map<String, Object> resolveGatewayResult = gatewayResolver.resolveGateway(scope);
                if (resolveGatewayResult.containsKey("error")) {
                    logger.info("Error while resolving gw for "+ meterSn);
                    return Map.of("error", resolveGatewayResult.get("error"));
                }
                if (resolveGatewayResult.containsKey("result")) {
                    gw = (String) resolveGatewayResult.get("result");
                    if (gw == null || gw.isEmpty()) {
                        return Map.of("error", "failed to resolve gateway for project: " + scope.get("project_name") + ", site: " + scope.get("site_tag"));
                    }
                } else {
                    return Map.of("error", "failed to resolve gateway for project: " + scope.get("project_name") + ", site: " + scope.get("site_tag"));
                }

                String mid = UUID.randomUUID().toString();
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("topic_pub", "evs2/nus/vh/" + gw);
                payload.put("meter_sn", meterSn);
                payload.put("cmd", acControllerCommand);
                payload.put("mid", mid);
                logger.info("submitServiceOnOff payload: " + payload);

                QueryReqDto<Map<String, Object>> req = new QueryReqDto<>(payload);
                StdRespDto stdRespDto = pagMqttAgentHelper.pmaR(cred, req, "/evs2/submit_ac_controller_command");

//                if (result.get("info") != null) {
//                    return result;
//                }
//                if (result.get("error") != null) {
//                    return result;
//                }
                return Map.of("std_resp_dto", stdRespDto);
            } catch (Exception e) {
                return Map.of("error", e.getMessage());
            }
        }
    }

    public Map<String, Object> checkAcControllerStatus(String meterSn, SvcClaimDto svcClaimDto){
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
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");

            //resolve gateway
            String gw = null;
            Map<String, Object> scope = Map.of(
                    "project_name", "evs2_nus",
                    "site_tag", "vh"
            );
            Map<String, Object> resolveGatewayResult = gatewayResolver.resolveGateway(scope);
            if (resolveGatewayResult.containsKey("error")) {
                logger.info("Error while resolving gw for "+ meterSn);
                return Map.of("error", resolveGatewayResult.get("error"));
            }
            if (resolveGatewayResult.containsKey("result")) {
                gw = (String) resolveGatewayResult.get("result");
                if (gw == null || gw.isEmpty()) {
                    return Map.of("error", "failed to resolve gateway for project: " + scope.get("project_name") + ", site: " + scope.get("site_tag"));
                }
            } else {
                return Map.of("error", "failed to resolve gateway for project: " + scope.get("project_name") + ", site: " + scope.get("site_tag"));
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("topic_pub", "evs2/nus/vh/" + gw);
            payload.put("topic_sub", "evs2/nus/vh/" + gw + "/" + meterSn);
            payload.put("meter_sn", meterSn);
            payload.put("cmd", "check_service_status");
            payload.put("get_response", "true");
            payload.put("mid", UUID.randomUUID().toString());
            logger.info("checkAcControllerStatus payload: " + payload);

            QueryReqDto<Map<String, Object>> req = new QueryReqDto<>(payload);
            try {
                StdRespDto stdRespDto = pagMqttAgentHelper.pmaR(cred, req, "/evs2/submit_ac_controller_command");
//                logger.info("checkAcControllerStatus stdRespDto to Map: " + stdRespDto.toMap());
//                if (result.get("info") != null) {
//                    return result;
//                }
//                if (result.get("error") != null) {
//                    return result;
//                }
                if (!stdRespDto.isSuccess() && stdRespDto.getError() != null) {
                    return Map.of("error", stdRespDto.getError());
                }
                return Map.of("std_resp_dto", stdRespDto);
            } catch (Exception e) {
                return Map.of("error", e.getMessage());
            }
        }
    }
}
