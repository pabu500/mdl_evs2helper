package org.pabuff.evs2helper;

import com.pabu5h.evs2.dto.*;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.apache.logging.log4j.LogManager.getLogger;

@Service
public class M3Helper {
    @Autowired
    RestTemplate restTemplate;

    @Value("${m3.path}")
    public String m3Path;

    @Autowired
    private MeterHelper meterHelper;
//    @Autowired
//    private M3Helper m3Helper;

    @Value("${m3.ept.mms_status}")
    public String m3EptMmsStatus;
    @Value("${m3.ept.get_meter_data}")
    public String m3EptGetMeterData;
    @Value("${m3.ept.query_meter_relay_status}")
    public String m3EptGetMeterRLS;
    @Value("${m3.ept.submit_relay_on_off}")
    public String m3EptSubmitRelayOnOff;
    @Value("${m3.ept.query_meter_comm_status}")
    public String m3EptGetMeterComm;
    @Value("${m3.ept.turn_meter_on_off}")
    public String m3EptTurnMeterOnOff;
    @Value("${m3.ept.check_rls_mid_subscribe}")
    public String m3EptCheckRlsMidSubscribe;

    @Value("${m3.ept.turn_ac_lock_on_off}")
    public String m3EptTurnAcLockOnOff;
    @Value("${m3.ept.check_ac_lock_status}")
    public String m3EptCheckAcLockStatus;

    private final Logger logger = getLogger(M3Helper.class);

    public M3ResponseDto<Object> M3R(QueryCredDto cred, QueryReqDto<Map<String, Object>> req, String ept) throws Exception {
        String m3GetMeterDataUrl = m3Path + ept;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> reqData = req.getReqData();

        QueryDto m3QueryDto = QueryDto.builder().credential(cred).request(reqData).build();

        HttpEntity<QueryDto> requestEntity = new HttpEntity<>(m3QueryDto, headers);
        try {
            ParameterizedTypeReference<M3ResponseDto<Object>> responseType = new ParameterizedTypeReference<>() {};

            ResponseEntity<M3ResponseDto<Object>> response = restTemplate.exchange(m3GetMeterDataUrl, HttpMethod.POST, requestEntity, responseType);

            if(response.getStatusCode() == HttpStatus.OK){
                return response.getBody();
            }else{
                throw new Exception("M3 Query Error: " + response.getStatusCode());
            }
        } catch (Exception e){
            throw new Exception("M3 Query Error: " + e.getMessage());
        }
    }

    public Map<String, Object> getMmsStatus(SvcClaimDto svcClaimDto) {

        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of());
            resp = M3R(cred, m3req, m3EptMmsStatus);

            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            if(resp.getResult()!= null) {
                Map<String, Object> mmsStatus = (Map<String, Object>) resp.getResult();
                return Collections.singletonMap("mms_status", mmsStatus);
            }
            return Collections.singletonMap("error", "unknown error");
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }

    public Map<String, Object> getMeterData(String meterDisplayname, String meterSn, SvcClaimDto svcClaimDto) {

        //meter_sn is compulsory for meter data
        if(meterSn == null || meterSn.isBlank()){
            Map<String, Object> respMeterSn = meterHelper.getMeterSnFromDisplayName(meterDisplayname);
            if (respMeterSn.containsKey("info")) {
                return respMeterSn;
            }
            if (respMeterSn.containsKey("error")) {
                return respMeterSn;
            }
            meterSn = Objects.requireNonNull(respMeterSn.get("meter_sn")).toString();
        }
        //meter_displayname is optional for meter data
        //A meter must have sn, but not necessarily have displayname
        if(meterDisplayname == null || meterDisplayname.isBlank()){
            Map<String, Object> respMeterDisplayname = meterHelper.getMeterDisplayNameFromSn(meterSn);
            if (respMeterDisplayname.containsKey("info")) {
//                return respMeterDisplayname;
                meterDisplayname = "";
            }else if (respMeterDisplayname.containsKey("error")) {
//                return respMeterDisplayname;
                meterDisplayname = "";
            }else {
                meterDisplayname = Objects.requireNonNull(respMeterDisplayname.get("meter_displayname")).toString();
            }
        }

        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of("meter_sn", meterSn));
            resp = M3R(cred, m3req, m3EptGetMeterData);

            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            if(resp.getSuccess()!= null) {
                Map<String, Object> meterData = (Map<String, Object>) resp.getResult();
                meterData.put("meter_displayname", meterDisplayname);
                return Collections.singletonMap("meter_data", meterData);
            }
            return Collections.singletonMap("result", resp.toString());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
    public Map<String, Object> getMeterRLS(String meterSn, long respMillis, SvcClaimDto svcClaimDto) {

        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of("meter_sn", meterSn, "resp_millis", respMillis));
            resp = M3R(cred, m3req, m3EptGetMeterRLS);

            if(resp.getResult()!= null) {
                return Collections.singletonMap("meter_rls", resp.getRls());
            }
            if(resp.getError()!= null){
//                return Collections.singletonMap("error", resp.getError());
                return Map.of("error", resp.getError(), "mid", resp.getRlsMid());
            }
            return Collections.singletonMap("result", resp.toString());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
//            return Map.of("error", e.getMessage(), "mid", resp.getRlsMid());
        }
    }
    public Map<String, Object> checkRlsMidSubscribe(String meterSn, String mid, SvcClaimDto svcClaimDto) {
        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of("meter_sn", meterSn, "mid", mid));
            resp = M3R(cred, m3req, m3EptCheckRlsMidSubscribe);

            if(resp.getResult()!= null) {
                return Collections.singletonMap("meter_rls", resp.getRls());
            }
            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            return Collections.singletonMap("result", resp.toString());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
    public Map<String, Object> getMeterComm(String meterSn, long respMillis, SvcClaimDto svcClaimDto) {

        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of("meter_sn", meterSn, "resp_millis", respMillis));
            resp = M3R(cred, m3req, m3EptGetMeterComm);

            if(resp.getResult()!= null) {
                Map<String, String> meterComm = Map.of("result", resp.getResult().toString(), "status", resp.getStatus()==null?"":resp.getStatus());
                return Collections.singletonMap("meter_comm", meterComm);
            }
            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            return Collections.singletonMap("result", resp.toString());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
    public Map<String, Object> turnMeterOnOff(String meterSn, String opTarget, long respMillis, SvcClaimDto svcClaimDto) {
        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of(
                    "meter_sn", meterSn,
                    "op_target", opTarget,
                    "resp_millis", respMillis));
            resp = M3R(cred, m3req, m3EptTurnMeterOnOff);

            if(resp.getResult()!= null) {
                return Collections.singletonMap("result_rls", resp.getRls());
            }
            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            return Collections.singletonMap("result", resp.toString());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
    public Map<String, Object> submitRelayOnOff(String meterSn, String opTarget, SvcClaimDto svcClaimDto) {
        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of(
                    "meter_sn", meterSn,
                    "op_target", opTarget));
            resp = M3R(cred, m3req, m3EptSubmitRelayOnOff);

//            if(resp.getResult()!= null) {
//                return Collections.singletonMap("result", resp.getResult());
//            }
            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            return Collections.singletonMap("result", resp.getResult());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
    public Map<String, Object> turnAcLockOnOff(String meterSn, String command, SvcClaimDto svcClaimDto) {
        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of(
                    "meter_sn", meterSn,
                    "command", command));
            resp = M3R(cred, m3req, m3EptTurnAcLockOnOff);

            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            //M3 return 'success' or 'error' field for this endpoint
            if(resp.getSuccess()!= null) {
                return Collections.singletonMap("result", resp.getSuccess());
            }
            return Collections.singletonMap("result", resp.toString());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
    public Map<String, Object> checkAcLockStatus(String meterSn, long respMillis, SvcClaimDto svcClaimDto) {
        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of(
                    "meter_sn", meterSn,
                    "resp_millis", respMillis));
            resp = M3R(cred, m3req, m3EptCheckAcLockStatus);

            if(resp.getError()!= null){
                return Collections.singletonMap("error", resp.getError());
            }
            return Collections.singletonMap("result", resp.getResult());
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
    }
}
