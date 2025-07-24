package org.pabuff.evs2helper.cpc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.logging.Logger;

@Component
public class GatewayResolver {
    Logger logger = Logger.getLogger(GatewayResolver.class.getName());

    @Value("${ac_controller_gateway_tag}")
    private String acControllerGatewayTag;

    public Map<String, Object> resolveGateway(Map<String, Object> scope) {
        logger.info("resolveGateway() called");

        String projectNameEvs2Nus = "evs2_nus";
        String siteTagVh = "vh";

        String projectName = (String) scope.get("project_name");
        String siteTag = (String) scope.get("site_tag");
        if (projectName == null || siteTag == null) {
            logger.warning("Project name or site tag is null");
            return null;
        }
        if (projectNameEvs2Nus.equalsIgnoreCase(projectName) && siteTagVh.equalsIgnoreCase(siteTag)) {
            String gw = acControllerGatewayTag;
            if(gw.isEmpty()){
                logger.warning("ac_controller_gateway_tag is empty");
                return Map.of("error", "ac_controller_gateway_tag is empty");
            }

            // randomly select one of the two
            if("random".equalsIgnoreCase(acControllerGatewayTag)) {
                gw = Math.random() < 0.5 ? "gw1" : "gw2";
            }

            logger.info("resolveGateway() returning: " + gw);
            return Map.of("result", gw);
        } else {
            logger.warning("failed to resolve gateway for project: " + projectName + ", site: " + siteTag);
            return Map.of(
                "error", "failed to resolve gateway for project: " + projectName + ", site: " + siteTag);
        }
    }
}
