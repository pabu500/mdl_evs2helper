package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.M3ResponseDto;
import com.pabu5h.evs2.dto.QueryCredDto;
import com.pabu5h.evs2.dto.QueryReqDto;
import com.pabu5h.evs2.dto.SvcClaimDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@Service
public class M3Gate {
    @Autowired
    private MeterHelper meterHelper;
    @Autowired
    private M3Helper m3Helper;

    public Map<String, Object> getMmsStatus(SvcClaimDto svcClaimDto) {

        M3ResponseDto<Object> resp;
        try {
            QueryCredDto cred = new QueryCredDto(svcClaimDto.getUsername(), "0");
            QueryReqDto<Map<String, Object>> m3req = new QueryReqDto<Map<String, Object>>(Map.of());
            resp = m3Helper.M3R(cred, m3req, m3Helper.m3EptMmsStatus);

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
            resp = m3Helper.M3R(cred, m3req, m3Helper.m3EptGetMeterData);

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
            resp = m3Helper.M3R(cred, m3req, m3Helper.m3EptGetMeterRLS);

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
            resp = m3Helper.M3R(cred, m3req, m3Helper.m3EptGetMeterComm);

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
            resp = m3Helper.M3R(cred, m3req, m3Helper.m3EptTurnMeterOnOff);

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
            resp = m3Helper.M3R(cred, m3req, m3Helper.m3EptSubmitRelayOnOff);

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
}
