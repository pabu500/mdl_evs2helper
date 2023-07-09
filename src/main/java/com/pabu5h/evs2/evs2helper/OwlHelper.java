package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.MeterInfoDto;
import com.pabu5h.evs2.dto.PremiseDto;
import com.xt.utils.MathUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class OwlHelper {
    private static final Logger logger = Logger.getLogger(OwlHelper.class.getName());
    @Value("${owl.path}")
    private String owlPath;
    @Value("${owl.ept.get_meter_info_list}")
    private String owlEptGetMeterInfoList;
    @Value("${owl.ept.get_meter_info_from_meter_sn}")
    private String owlEptGetMeterInfoFromMeterSn;
    @Value("${owl.ept.get_meter_info_from_meter_displayname}")
    private String owlEptGetMeterInfoFromMeterDisplayname;
    @Value("${owl.ept.sync_mms_meter_batch}")
    private String syncMmsMeterBatchEpt;
    @Autowired
    RestTemplate restTemplate;

    public Map<String, Object> syncMmsMeterBatch(List<String> meterSnList) {
        //http get request to owl
        String url = owlPath + syncMmsMeterBatchEpt;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<List<String>> requestEntity = new HttpEntity<>(meterSnList, headers);

        //send request
        ResponseEntity<Map> resp = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Map.class);
        if (resp.getStatusCode() != HttpStatus.OK) {
            return Collections.singletonMap("error", resp.getBody());
        }
        return resp.getBody();
    }

    public Map<String, Object> getMeterInfoList(){
        String url = owlPath + owlEptGetMeterInfoList;

        ResponseEntity<Map> resp = restTemplate.exchange(url, HttpMethod.GET, null, Map.class);
        if (resp.getStatusCode() != HttpStatus.OK) {
            logger.info("getMeterInfoList() error: " + resp.getBody());
            return Collections.singletonMap("error", resp.getBody());
        }
        if(resp.getBody().containsKey("meter_info_list")) {
            return resp.getBody();
        }
        return Collections.singletonMap("error", "unknown error");
    }
    public Map<String, Object> getMeterInfoFromMeterSn(String meterSn) {
        //http get request to owl
        String url = owlPath + owlEptGetMeterInfoFromMeterSn + "?meter_sn=" + meterSn;

        //send request
        ResponseEntity<Map> resp = restTemplate.exchange(url, HttpMethod.GET, null, Map.class);
        if (resp.getStatusCode() != HttpStatus.OK) {
            return Collections.singletonMap("error", resp.getBody());
        }
        if(resp.getBody().containsKey("meter_info")) {
            Map<String, Object> respMap = (Map<String, Object>) resp.getBody().get("meter_info");
            MeterInfoDto meterInfoDto = getMeterInfoDto(respMap);
            return Map.of("meter_info", meterInfoDto);
        }
        return Collections.singletonMap("error", "unknown error");
    }
    public Map<String, Object> getMeterInfoFromMeterDisplayname(String meterDisplayname) {
        //http get request to owl
        String url = owlPath + owlEptGetMeterInfoFromMeterDisplayname + "?meter_displayname=" + meterDisplayname;

        //send request
        ResponseEntity<Map> resp = restTemplate.exchange(url, HttpMethod.GET, null, Map.class);
        if (resp.getStatusCode() != HttpStatus.OK) {
            return Collections.singletonMap("error", resp.getBody());
        }
        if(resp.getBody().containsKey("meter_info")) {
            Map<String, Object> respMap = (Map<String, Object>) resp.getBody().get("meter_info");
            MeterInfoDto meterInfoDto = getMeterInfoDto(respMap);
            return Map.of("meter_info", meterInfoDto);
        }
        return Collections.singletonMap("error", "unknown error");
    }
    private MeterInfoDto getMeterInfoDto(Map<String, Object> respMap){
        return MeterInfoDto.builder()
                .meterDisplayname((String) respMap.get("meter_displayname"))
                .concentratorId((String) respMap.get("concentrator_id"))
                .meterSn((String) respMap.get("meter_sn"))
                .address((String) respMap.get("address"))
                .premise(PremiseDto.builder()
                        .level(((Map<String, String>)respMap.get("premise")).get("level"))
                        .block(((Map<String, String>)respMap.get("premise")).get("block"))
                        .building(((Map<String, String>)respMap.get("premise")).get("building"))
                        .build())
                .readingInterval(respMap.get("reading_interval")==null?0:(MathUtil.ObjToLong(respMap.get("reading_interval"))))
                .build();
    }

}
