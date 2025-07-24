package org.pabuff.evs2helper.mqtt;

import org.pabuff.dto.*;
import org.pabuff.utils.ApiCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.logging.Logger;

@Component
public class PagMqttAgentHelper {
    Logger logger = Logger.getLogger(PagMqttAgentHelper.class.getName());

    @Autowired
    RestTemplate restTemplate;

    @Value("${pag.mqtt.agent.path}")
    public String pagMqttAgentPath;

    public StdRespDto pmaR(QueryCredDto cred, QueryReqDto<Map<String, Object>> req, String ept) {
        logger.info("pmaR called");

        String url = pagMqttAgentPath + ept;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> reqData = req.getReqData();

        QueryDto queryDto = QueryDto.builder().credential(cred).request(reqData).build();

        HttpEntity<QueryDto> requestEntity = new HttpEntity<>(queryDto, headers);
//        ParameterizedTypeReference<Map<String, Object>> responseType = new ParameterizedTypeReference<>() {};
        try {
            ResponseEntity<StdRespDto> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, StdRespDto.class);

            if (response.getStatusCode() == HttpStatus.OK) {
                logger.info("pmaR Response OK: " + response.getBody());
                return response.getBody();
            } else {
                logger.info("pmaR Response Error: " + response.getBody());
//                return Map.of("error", response.getBody() == null ? response.getStatusCode() : response.getBody().toString());
                return response.getBody();
            }
        } catch (RestClientResponseException e) {
            String responseBody = e.getResponseBodyAsString();
            logger.severe("HTTP Status: " + e.getStatusCode());
            logger.severe("Raw Response Body: " + responseBody);

            StdErrorDto stdErrorDto = StdErrorDto.builder()
                    .code(ApiCode.RESULT_GENERIC_ERROR)
                    .message("Failed to parse response: " + responseBody)
                    .build();
            return StdRespDto.builder().error(stdErrorDto).build();
        } catch (Exception ex) {
            logger.severe("Unexpected Exception: " + ex.getMessage());
            StdErrorDto stdErrorDto = StdErrorDto.builder()
                    .code(ApiCode.RESULT_GENERIC_ERROR)
                    .message(ex.getMessage())
                    .build();
            return StdRespDto.builder().error(stdErrorDto).build();
        }
    }

}
