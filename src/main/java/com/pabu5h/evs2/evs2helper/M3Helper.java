package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.M3ResponseDto;
import com.pabu5h.evs2.dto.QueryCredDto;
import com.pabu5h.evs2.dto.QueryDto;
import com.pabu5h.evs2.dto.QueryReqDto;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

import static org.apache.logging.log4j.LogManager.getLogger;

@Service
public class M3Helper {
    @Autowired
    RestTemplate restTemplate;

    @Value("${m3.path}")
    public String m3Path;

    @Value("${m3.ept.mms_status}")
    public String m3EptMmsStatus;

    @Value("${m3.ept.get_meter_data}")
    public String m3EptGetMeterData;
    @Value("${m3.ept.query_meter_relay_status}")
    public String m3EptGetMeterRLS;
    @Value("${m3.ept.query_meter_comm_status}")
    public String m3EptGetMeterComm;
    @Value("${m3.ept.turn_meter_on_off}")
    public String m3EptTurnMeterOnOff;

    private final Logger logger = getLogger(M3Helper.class);
//    public M3Helper(Logger logger) {
//        this.logger = logger;
//    }

    public M3ResponseDto<Object> M3R(QueryCredDto cred, QueryReqDto<Map<String, Object>> req, String ept) throws Exception {
        String m3GetMeterDataUrl = m3Path + ept;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> reqData = req.getReqData();

//        if(ept.equals(m3EptGetMeterData)){
//            reqData.put("meter_sn", reqData.get("meter_sn"));
//        }
//        if(ept.equals(m3EptTurnMeterOnOff)){
//            reqData.put("meter_sn", reqData.get("meter_sn"));
//            reqData.put("op_target", reqData.get("op_target"));
//            reqData.put("resp_millis", reqData.get("resp_millis"));
//
//        }
//        if(ept.equals(m3EptGetMeterRLS)){
//            reqData.put("meter_sn", reqData.get("meter_sn"));
//            reqData.put("resp_millis", reqData.get("resp_millis"));
//        }
//
//
//        Map<String, Object> request = Map.of("meter_sn", reqData.get("meter_sn"));
//        if(ept.equals(m3EptGetMeterRLS)){
//            request = Map.of("meter_sn", reqData.get("meter_sn"), "resp_millis", reqData.get("resp_millis"));
//        }

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
}
