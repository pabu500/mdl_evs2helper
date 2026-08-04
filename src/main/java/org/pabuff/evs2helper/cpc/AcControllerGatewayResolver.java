package org.pabuff.evs2helper.cpc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class AcControllerGatewayResolver {
    Logger logger = Logger.getLogger(AcControllerGatewayResolver.class.getName());

    @Value("${ac_controller_gateway_tag}")
    private String acControllerGatewayTag;
    private static final List<String> siteTagNus5HallsList = List.of("nus_eh", "nus_ke7h", "nus_krh", "nus_sh", "nus_th");

    public Map<String, Object> resolveGateway(Map<String, Object> scope) {
        logger.info("resolveGateway()");

        String projectNameEvs2Nus = "evs2_nus";
        String siteTagVh = "vh";
        String siteTagPgpr = "pgpr";

        String projectName = (String) scope.get("project_name");
        String siteTag = (String) scope.get("site_tag");
        if (projectName == null || siteTag == null) {
            logger.warning("Project name or site tag is null");
            return null;
        }
        if (projectNameEvs2Nus.equalsIgnoreCase(projectName) && (siteTag.contains(siteTagVh)
                || siteTag.contains(siteTagPgpr) || containNus5Halls(siteTag))) {
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

    public Map<String, Object> resolveGatewayTopic(Map<String, Object> scope, Map<String, Object> meterInfo, String gw) {
        logger.info("resolveGatewayTopic()");

        String projectName = (String) scope.get("project_name");
        String siteTag = (String) scope.get("site_tag");
        if(projectName == null || siteTag == null) {
            logger.warning("Project name or site tag is null");
            return Map.of("error", "Project name or site tag is null");
        }

        String meterSn = (String) meterInfo.get("meter_sn");
        if(meterSn == null) {
            logger.warning("Meter SN is null");
            return Map.of("error", "Meter SN is null");
        }
        String block  = (String) meterInfo.get("mms_block");

        Map<String, Object> result = new HashMap<>();
        String topicPublish;
        String topicSubscribe;
        switch (siteTag) {
            case "nus_vh":
                topicPublish = "evs2/nus/vh/" + gw;
                topicSubscribe = "evs2/nus/vh/" + gw + "/" + meterSn;
                break;
            case "nus_pgpr":
                topicPublish = "evs2/nus/pgpr" + block + "/" + gw;
                topicSubscribe = "evs2/nus/pgpr" + block + "/" + gw + "/" + meterSn;
                break;
            case "nus_eh", "nus_ke7h", "nus_krh", "nus_sh", "nus_th":
                String site = siteTag.split("_")[1];
                block = block.toLowerCase();
                topicPublish = "evs2/nus/" + site + block + "/" + gw;
                topicSubscribe = "evs2/nus/" + site + block + "/" + gw + "/" + meterSn;
                break;
            default:
                topicPublish = null;
                topicSubscribe = null;
        }

        if(topicPublish == null || topicSubscribe == null) {
            return Map.of("error", "failed to resolve topic for project: " + projectName + ", site: " + siteTag);
        }

        logger.info("resolveTopic() returning: " + topicPublish + ", topicSubscribe: " + topicSubscribe);
        result.put("topic_pub", topicPublish);
        result.put("topic_sub", topicSubscribe);
        return Map.of("result", result);
    }

    private boolean containNus5Halls(String siteTag) {
        for (String nus5hallSiteTag : siteTagNus5HallsList) {
            if (siteTag.contains(nus5hallSiteTag)) {
                return true;
            }
        }
        return false;
    }
}
