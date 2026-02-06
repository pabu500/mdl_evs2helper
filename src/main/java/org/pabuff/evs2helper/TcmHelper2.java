package org.pabuff.evs2helper;

import org.pabuff.dto.TransactionLogDto;
import org.pabuff.oqghelper.OqgHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
public class TcmHelper2 {
    @Value("${tcm.path}")
    private String tcmPath;
    @Value("${tcm.path.nus_pgpr}")
    private String tcmPathNusPgpr;
    @Value("${tcm.path.nus_utown}")
    private String tcmPathNusUtown;
    @Value("${tcm.path.nus_ync}")
    private String tcmPathNusYnc;
    @Value("${tcm.path.nus_rvrc}")
    private String tcmPathNusRvrc;
    @Value("${tcm.path.nus_vh}")
    private String tcmPathNusVh;
    @Value("${tcm.path.ntu_mr}")
    private String tcmPathNtuMr;

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
    @Autowired
    private OqgHelper oqgHelper;

    public Map<String, Object> doOneTopup(TransactionLogDto transactionLogDto) throws Exception {
        String meterDisplayname = transactionLogDto.getMeterDisplayname();
        String tcmEpt = getTcmPath("meter_displayname", meterDisplayname) + tcmEptDoOneTopup;

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
        String tcmEpt = tcmPath + tcmEptDoBatchTopup;

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
        String meterSn = (String) resetMap.get("meter_sn");
        String tcmEpt = getTcmPath("meter_sn", meterSn) + tcmEptDoOneResetRefBal;

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

    private String getTcmPath(String meterKeyName, String meterKeyValue) {
        String sql = "SELECT site_tag FROM meter WHERE " + meterKeyName + " = '" + meterKeyValue + "'";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2x(sql, "EVS2", true);
        } catch (Exception e) {
            return tcmPath;
        }
        if (resp != null && !resp.isEmpty()) {
            String siteTag = (String) resp.getFirst().get("site_tag");
            if ("nus_pgpr".equals(siteTag)) {
                return tcmPathNusPgpr;
            } else if ("nus_utown".equals(siteTag)) {
                return tcmPathNusUtown;
            } else if ("nus_ync".equals(siteTag)) {
                return tcmPathNusYnc;
            } else if ("nus_rvrc".equals(siteTag)) {
                return tcmPathNusRvrc;
            } else if ("nus_vh".equals(siteTag)) {
                return tcmPathNusVh;
            } else if ("ntu_mr".equals(siteTag)) {
                return tcmPathNtuMr;
            }
        }
        return tcmPath;
    }

    public Map<String, Object> doBatchResetRefBal(List<Map<String, Object>> resetList) throws Exception {
        // get sn from the first item to determine tcm path
        String meterSn = (String) resetList.getFirst().get("meter_sn");
        String tcmPath = getTcmPath("meter_sn", meterSn);

        String tcmEpt = tcmPath + tcmEptDoBatchResetRefBal;

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
        // get sn from the first item to determine tcm path
        String meterSn = meterSns.getFirst();
        String tcmPath = getTcmPath("meter_sn", meterSn);
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
