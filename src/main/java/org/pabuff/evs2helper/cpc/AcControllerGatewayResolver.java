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

    // 48 meters at PGPR 6 are controlled by the ac controller at pgpr 5
    static private final List<String> pgpr6AdhocMeter = List.of(
            "201808000338",
            "201808000340",
            "201808000268",
            "201808000271",
            "201808000270",
            "201808000247",
            "201808000336",
            "201808000253",
            "201808000255",
            "201906000050",
            "201906000139",
            "201906000044",
            "201906000370",
            "201906000382",
            "201906000138",
            "201906000134",
            "201906000381",
            "201906000385",
            "201906000029",
            "201906000017",
            "201906000028",
            "201906000019",
            "201906000025",
            "201906000201",
            "201906000380",
            "201906000132",
            "201906000042",
            "201906000265",
            "201906000267",
//            "201906000205",
            "201906000023",
            "201906000207",
            "201906000383",
            "201906000337",
            "201906000336",
            "201906000319",
            "201906000317",
            "201906000188",
            "201906000186",
            "201906000192",
            "201906000193",
            "201906000327",
            "201906000325",
            "201906000181",
            "201906000187",
            "201906000144",
            "201906000146",
            "201906000081",
            "201906000077");


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
                if("6".equals(block)){
                    if(pgpr6AdhocMeter.contains(meterSn)){
                        block = "5";
                    }
                }

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
