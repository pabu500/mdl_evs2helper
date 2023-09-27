package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.TransactionLogDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
public class TcmHelper {
    @Value("${tcm.path}")
    private String tcmPath;
    @Value("${tcm.ept.do_one_topup}")
    private String tcmEptDoOneTopup;
    @Value("${tcm.ept.do_batch_topup}")
    private String tcmEptDoBatchTopup;
    @Value("${tcm.ept.do_one_reset_ref_bal}")
    private String tcmEptDoOneResetRefBal;
    @Value("${tcm.ept.do_batch_reset_ref_bal}")
    private String tcmEptDoBatchResetRefBal;
    @Value("${tcm.ept.update_meter_bypass_policy}")
    private String tcmEptUpdateMeterBypassPolicy;

    @Autowired
    private RestTemplate restTemplate;

    public Map<String, Object> doOneTopup(TransactionLogDto transactionLogDto) throws Exception {
        String tcmEpt = tcmPath + tcmEptDoOneTopup;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<TransactionLogDto> requestEntity = new HttpEntity<>(transactionLogDto, headers);

        try {
            ResponseEntity<Map<String, Object>> response = this.restTemplate.exchange(tcmEpt, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, Object>>() {});
            if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            } else {
                throw new Exception("TCM Query Error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            throw new Exception("TCM Query Error: " + e.getMessage());
        }
    }
    public Map<String, Object> doBatchTopup(List<Map<String, Object>> topupList) throws Exception {
        String tcmEpt = tcmPath + tcmEptDoOneTopup;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<List<Map<String, Object>>> requestEntity = new HttpEntity<>(topupList, headers);

        try {
            ResponseEntity<Map<String, Object>> response = this.restTemplate.exchange(tcmEpt, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, Object>>() {});
            if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            } else {
                throw new Exception("TCM Query Error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            throw new Exception("TCM Query Error: " + e.getMessage());
        }
    }
    public Map<String, Object> resetOneRefBal(Map<String, Object> resetMap) throws Exception {
        String tcmEpt = tcmPath + tcmEptDoOneResetRefBal;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(resetMap, headers);

        try {
            ResponseEntity<Map<String, Object>> response = this.restTemplate.exchange(tcmEpt, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, Object>>() {});
            if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            } else {
                throw new Exception("TCM Query Error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            throw new Exception("TCM Query Error: " + e.getMessage());
        }
    }
    public Map<String, Object> doBatchResetRefBal(List<Map<String, Object>> resetList) throws Exception {
        String tcmEpt = tcmPath + tcmEptDoOneResetRefBal;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<List<Map<String, Object>>> requestEntity = new HttpEntity<>(resetList, headers);

        try {
            ResponseEntity<Map<String, Object>> response = this.restTemplate.exchange(tcmEpt, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, Object>>() {});
            if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            } else {
                throw new Exception("TCM Query Error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            throw new Exception("TCM Query Error: " + e.getMessage());
        }
    }
    public Map<String, Object> updateMeterBypassPolicy(List<String> meterSns) throws Exception {
        String tcmEpt = tcmPath + tcmEptUpdateMeterBypassPolicy;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<List<String>> requestEntity = new HttpEntity<>(meterSns, headers);

        try {
            ResponseEntity<Map<String, Object>> response = this.restTemplate.exchange(tcmEpt, HttpMethod.POST, requestEntity, new ParameterizedTypeReference<Map<String, Object>>() {});
            if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            } else {
                throw new Exception("TCM Query Error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            throw new Exception("TCM Query Error: " + e.getMessage());
        }
    }
}
