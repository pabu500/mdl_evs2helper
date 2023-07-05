package com.pabu5h.evs2.evs2helper;

import com.pabu5h.evs2.dto.TransactionLogDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class TcmHelper {
    @Value("${tcm.path}")
    private String tcmPath;
    @Value("${tcm.etp.insert_one_credit}")
    private String tcmEtpInsertOneCredit;
    @Value("${tcm.etp.reset_one_ref_bal}")
    private String tcmEtpResetOneRefBal;

    @Autowired
    private RestTemplate restTemplate;

    public Map<String, Object> insertOnceCredit(TransactionLogDto transactionLogDto) throws Exception {
        String tcmEpt = tcmPath + tcmEtpInsertOneCredit;

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

    public Map<String, Object> resetOneRefBal(Map<String, Object> resetMap) throws Exception {
        String tcmEpt = tcmPath + tcmEtpResetOneRefBal;

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
}
