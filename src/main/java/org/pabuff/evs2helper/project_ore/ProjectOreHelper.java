package org.pabuff.evs2helper.project_ore;

import org.pabuff.dto.StdErrorDto;
import org.pabuff.dto.StdRespDto;
//import org.pabuff.paghelper.scope.PagScope;
//import org.pabuff.paghelper.scope.ScopeProcessor;
import org.pabuff.evs2helper.scope.PagScope;
import org.pabuff.evs2helper.scope.ScopeProcessor;
import org.pabuff.utils.ApiCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.logging.Logger;

@Component
public class ProjectOreHelper {
    Logger logger = Logger.getLogger(ProjectOreHelper.class.getName());

    // 3 values: dev, deploy-test, deploy
    @Value("${project.ore.type}")
    private String projectOreType;

    @Value("${poh.ept.get_role_scope}")
    private String eptGetRoleScope;
    @Value("${poh.ept.get_site_list}")
    private String eptGetSiteList;
    @Value("${poh.ept.get_project_info}")
    private String eptGetProjectInfo;
    @Value("${poh.ept.get_list_info_list}")
    private String eptGetListInfoList;
    @Value("${poh.ept.get_scope_device_list}")
    private String eptGetScopeDeviceList;
//    @Value("${poh.ept.get_filter_list}")
//    private String eptGetFilterList;
//    @Value("${poh.ept.get_item_history}")
//    private String eptGetItemHistory;
    @Value("${poh.ept.get_meter_period_total}")
    private String eptGetMeterPeriodTotal;
    @Value("${poh.ept.get_meter_history_snapshot}")
    private String eptGetMeterHistorySnapshot;
    @Value("${poh.ept.get_scada_data}")
    private String eptGetScadaData;
    @Value("${poh.ept.get_scada_data_point_def}")
    private String eptGetScadaDataPointDef;
    @Value("${poh.ept.get_scada_info}")
    private String eptGetScadaInfo;
    @Value("${poh.ept.get_fleet_health}")
    private String eptGetFleetHealth;
    @Value("${poh.ept.get_manual_meter_list}")
    private String getManualMeterList;
    @Value("${poh.ept.submit_manual_reading_list}")
    private String submitManualReadingList;

    @Autowired
    private ScopeProcessor scopeProcessor;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private RestTemplate restTemplateLong;

    public static String getProjectItemTableName(String projectName, String itemName) {
        return projectName + "." + itemName + "_" + projectName;
    }

    public Map<String, Object> getProjectOreUrl(Map<String, Object> request) {
        logger.info("getProjectOreUrl");

        Map<String, Object> result = scopeProcessor.getProjectOreRequest(request);
        if (result.get("error") != null) {
            return Map.of("error", result.get("error"));
        }

        String projectId = (String) result.get("project_id");
        String projectOreUrlDev = (String) result.get("ore_url_dev");
        String projectOreUrlDeployTest = (String) result.get("ore_url_deploy_test");
        String projectOreUrlDeploy = (String) result.get("ore_url_deploy");
        String basePath = (String) result.get("base_path");

        String projectOreUrl;
        if ("dev".equals(projectOreType)) {
            if (projectOreUrlDev == null || projectOreUrlDev.isEmpty()) {
                return Map.of("error", "No dev URL found");
            }
            projectOreUrl = projectOreUrlDev;
        } else if ("deploy-test".equals(projectOreType)) {
            if (projectOreUrlDeployTest == null || projectOreUrlDeployTest.isEmpty()) {
                return Map.of("error", "No deploy-test URL found");
            }
            projectOreUrl = projectOreUrlDeployTest;
        } else {
            if (projectOreUrlDeploy == null || projectOreUrlDeploy.isEmpty()) {
                return Map.of("error", "No deploy URL found");
            }
            projectOreUrl = projectOreUrlDeploy;
        }

        if (basePath != null && !basePath.isEmpty()) {
            projectOreUrl = projectOreUrl + basePath;
        }

        return Map.of("project_id", projectId, "ore_url", projectOreUrl);
    }

    public Map<String, Object> getProjectOreUrl2(PagScope pagScope) {
        logger.info("getProjectOreUrl()");

        Map<String, Object> result = scopeProcessor.getProjectOreRequest2(pagScope);
        if (result.get("error") != null) {
            return Map.of("error", result.get("error"));
        }

        String projectId = (String) result.get("project_id");
        String projectOreUrlDev = (String) result.get("ore_url_dev");
        String projectOreUrlDeployTest = (String) result.get("ore_url_deploy_test");
        String projectOreUrlDeploy = (String) result.get("ore_url_deploy");
        String basePath = (String) result.get("base_path");

        String projectOreUrl;
        if ("dev".equals(projectOreType)) {
            if (projectOreUrlDev == null || projectOreUrlDev.isEmpty()) {
                return Map.of("error", "No dev URL found");
            }
            projectOreUrl = projectOreUrlDev;
        } else if ("deploy-test".equals(projectOreType)) {
            if (projectOreUrlDeployTest == null || projectOreUrlDeployTest.isEmpty()) {
                return Map.of("error", "No deploy-test URL found");
            }
            projectOreUrl = projectOreUrlDeployTest;
        } else {
            if (projectOreUrlDeploy == null || projectOreUrlDeploy.isEmpty()) {
                return Map.of("error", "No prod URL found");
            }
            projectOreUrl = projectOreUrlDeploy;
        }

        if (basePath != null && !basePath.isEmpty()) {
            projectOreUrl = projectOreUrl + basePath;
        }

        return Map.of("project_id", projectId, "ore_url", projectOreUrl);
    }

    public Map<String, Object> exchange(Map<String, Object> request) {
        logger.info("exchange()");

        String endpoint = (String) request.get("endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            return Map.of("error", "No endpoint found in request");
        }

        String resultKey = (String) request.get("result_key");
        if (resultKey == null || resultKey.isEmpty()) {
            return Map.of("error", "No result_key found in request");
        }

        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
        if (scope == null || scope.isEmpty()) {
            return Map.of("error", "No scope found in request");
        }

        PagScope pagScope = PagScope.fromMap(scope);
        if(pagScope.getErrorMessage() != null) {
            return Map.of("error", pagScope.getErrorMessage());
        }

        Map<String, Object> result = getProjectOreUrl2(pagScope);
        if (result.get("error") != null) {
            return Map.of("error", result.get("error"));
        }

        String projectId = (String) result.get("project_id");
        String projectOreUrl = (String) result.get("ore_url");

        boolean useLongRequest = "true".equals(request.get("use_long_request"));

        String url = projectOreUrl + endpoint;
        logger.info("url: " + url);
        try {
            Map<String, Object> response = null;
            if(useLongRequest){
                logger.info("Using long request for " + url);
                response = restTemplateLong.postForObject(url, request, Map.class);
            }else {
                response = restTemplate.postForObject(url, request, Map.class);
            }
            if (response.containsKey("error")) {
                logger.severe("Error from " + url + ": " + response.get("error"));
                return response;
            }
            Map<String, Object> data = (Map<String, Object>) response.get("data");
            if (data == null) {
                return Map.of("error", "No data found in the response from " + url);
            }
            if (!data.containsKey(resultKey)) {
                return Map.of("error", "No " + resultKey + " found in the response data");
            }
            return data;
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    public StdRespDto exchange2(Map<String, Object> request) {
        logger.info("exchange2()");

        String endpoint = (String) request.get("endpoint");
        if (endpoint == null || endpoint.isEmpty()) {
            return StdRespDto.builder()
                    .error(StdErrorDto.builder()
                        .code(ApiCode.REQUEST_MISSING_PARAMETER)
                        .message("endpoint is required")
                        .build())
                    .build();
        }

        String resultKey = (String) request.get("result_key");
        if (resultKey == null || resultKey.isEmpty()) {
            return StdRespDto.builder()
                    .error(StdErrorDto.builder()
                        .code(ApiCode.REQUEST_MISSING_PARAMETER)
                        .message("No result_key found")
                        .build())
                    .build();
        }

        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
        if (scope == null || scope.isEmpty()) {
            return StdRespDto.builder()
                    .error(StdErrorDto.builder()
                        .code(ApiCode.REQUEST_MISSING_PARAMETER)
                        .message("scope is required")
                        .build())
                    .build();
        }

        PagScope pagScope = PagScope.fromMap(scope);
        if(pagScope.getErrorMessage() != null) {
//            return Map.of("error", pagScope.getErrorMessage());
            return StdRespDto.builder()
                    .error(StdErrorDto.builder()
                        .code(ApiCode.REQUEST_MISSING_PARAMETER)
                        .message(pagScope.getErrorMessage())
                        .build())
                    .build();
        }

        Map<String, Object> result = getProjectOreUrl2(pagScope);
        if (result.get("error") != null) {
//            return Map.of("error", result.get("error"));
            return StdRespDto.builder()
                    .error(StdErrorDto.builder()
                        .code(ApiCode.REQUEST_MISSING_PARAMETER)
                        .message((String) result.get("error"))
                        .build())
                    .build();
        }

        String projectId = (String) result.get("project_id");
        String projectOreUrl = (String) result.get("ore_url");

        boolean useLongRequest = "true".equals(request.get("use_long_request"));

        String url = projectOreUrl + endpoint;
        try {
            StdRespDto response = null;
            if(useLongRequest){
                logger.info("Using long request for " + url);
                response = restTemplateLong.postForObject(url, request, StdRespDto.class);
            }else {
                response = restTemplate.postForObject(url, request, StdRespDto.class);
            }
            return response;
        } catch (Exception e) {
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
