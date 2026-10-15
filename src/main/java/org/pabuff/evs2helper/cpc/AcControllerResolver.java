package org.pabuff.evs2helper.cpc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class AcControllerResolver {
    static final private Logger  logger = Logger.getLogger(AcControllerResolver.class.getName());

    @Value("${pag.mqtt.agent.vh.path}")
    public String pagMqttAgentVhPath;
    @Value("${pag.mqtt.agent.pgpr.path}")
    public String pagMqttAgentPgprPath;
    @Value("${pag.mqtt.agent.five_halls.path}")
    public String pagMqttAgentFiveHallsPath;
    private static final List<String> siteTagNus5HallsList = List.of("nus_eh", "nus_ke7h", "nus_krh", "nus_sh", "nus_th");

    // PGPR 6 meters controlled by the ac controller at PGPR 5, identified by display name
    static private final List<String> pgpr6AdhocMeterDisplayNames = List.of(
            "10010821",
            "10010822",
            "10010823",
            "10010824",
            "10010825",
            "10010829",
            "10010830",
            "10010831",
            "10010832",
            "10010854",
            "10010855",
            "10010856",
            "10010857",
            "10010858",
            "10010859",
            "10010860",
            "10010861",
            "10010862",
            "10010869",
            "10010870",
            "10010871",
            // Skipped meter SN 201906000019: display name is missing from the meter CSV.
            "10010888",
            "10010889",
            "10010898",
            "10010899",
            "10010900",
            "10010907",
            "10010908",
            "10010909",
            "10010910",
            "10010911",
            "10010921",
            "10010922",
            "10010923",
            "10010924",
            "10010943",
            "10010944",
            "10010945",
            "10010946",
            "10010947",
            "10010948",
            "10010958",
            "10010959",
            "10010960",
            "10010961",
            "10010962",
            "10010963",
            "10010873");

    public Map<String, Object> resolveAcControlInfo(Map<String, Object> meterInfo){
        logger.info("resolveAcControlInfo()");

        String meterDisplayName =  (String) meterInfo.get("meter_displayname");
        if(meterDisplayName == null || meterDisplayName.isBlank()){
            logger.warning("Meter display name is null or blank");
            return Map.of("error", "Meter display name is null or blank");
        }

        String siteTag =  (String) meterInfo.get("site_tag");
        if(siteTag == null){
            logger.warning("Site Tag is null");
            return Map.of("error", "Site Tag is null");
        }

        String block =  (String) meterInfo.get("block");
        String topicPub = "";
        String topicSub = "";
        String pmaPath = "";

        if("nus_vh".equalsIgnoreCase(siteTag)) {
            // # 1 send command to turn on/off service
            //resolve gateway
            String gw = null;
//            Map<String, Object> scope = Map.of(
//                    "project_name", "evs2_nus",
//                    "site_tag", "vh"
//            );
            // randomly select one of the two
            String acCtrlSel = "random";
            if ("random".equalsIgnoreCase(acCtrlSel)) {
                gw = Math.random() < 0.5 ? "gw1" : "gw2";
            }
            topicPub = "evs2/nus/vh/" + gw;
            topicSub = "evs2/nus/vh/" + gw + "/" + meterDisplayName;
            pmaPath = pagMqttAgentVhPath;
        } else if("nus_pgpr".equalsIgnoreCase(siteTag)){
            String gw = "gw1";
            String site = "pgpr";

            if("6".equals(block)){
                if(pgpr6AdhocMeterDisplayNames.contains(meterDisplayName)){
                    block = "5";
                }
            }

            String topicInfix = block != null && !block.isEmpty() ? site + block : site;
            topicPub = "evs2/nus/" + topicInfix + "/" + gw;
            topicSub = "evs2/nus/" + topicInfix + "/" + gw + "/" + meterDisplayName;
            pmaPath = pagMqttAgentPgprPath;
        } else if(isNus5HallsFromSiteTag(siteTag)){
            String gw = "gw1";
            block = block.toLowerCase();
            String site = siteTag.split("_")[1];

            String topicInfix = site + block;
            topicPub = "evs2/nus/" + topicInfix + "/" + gw;
            topicSub = "evs2/nus/" + topicInfix + "/" + gw + "/" + meterDisplayName;
            pmaPath = pagMqttAgentFiveHallsPath;
        } else {
            return Map.of("error", "Unsupported site tag: " + siteTag);
        }

        return Map.of("topic_pub", topicPub, "topic_sub", topicSub, "pma_path", pmaPath);
    }

    Boolean isNus5HallsFromSiteTag(String siteTag){
        if(siteTag == null || siteTag.isEmpty()){
            return false;
        }

        siteTag = siteTag.toLowerCase();
//        return "nus_eh".equals(siteTag) || "nus_ke7h".equals(siteTag) || "nus_krh".equals(siteTag)
//                || "nus_sh".equals(siteTag) || "nus_th".equals(siteTag);
        return siteTagNus5HallsList.contains(siteTag);
    }
}
