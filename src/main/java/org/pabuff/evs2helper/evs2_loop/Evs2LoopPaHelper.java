package org.pabuff.evs2helper.evs2_loop;

import org.pabuff.dto.StdErrorDto;
import org.pabuff.dto.StdRespDto;
import org.pabuff.utils.ApiCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.logging.Logger;

@Component
public class Evs2LoopPaHelper {
    Logger logger = Logger.getLogger(Evs2LoopPaHelper.class.getName());

    @Value("${project.ore.pa.path}")
    private String projectOrePaPath;
    @Value("${poh.pa.ept.resolve_gateway}")
    private String pohEptResolveGateway;

    @Autowired
    private RestTemplate restTemplate;

    public StdRespDto resolveGateway(String metreSn) {
        String url = projectOrePaPath + pohEptResolveGateway;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> scope = Map.of("project_name", "pa");
        Map<String, Object> request = Map.of(
                "scope", scope,
                "meter_sn", metreSn
        );

        HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(request, headers);
        try {
            ResponseEntity<StdRespDto> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity, StdRespDto.class);
            return response.getBody();
        } catch (Exception e){
            logger.severe(e.getMessage());
            return StdRespDto.builder()
                    .error(StdErrorDto.builder()
                            .code(ApiCode.RESULT_GENERIC_ERROR)
                            .message(e.getMessage())
                            .build())
                    .build();
        }
    }
}
