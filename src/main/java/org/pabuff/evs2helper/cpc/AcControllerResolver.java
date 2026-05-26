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
            "201906000205",
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

    public Map<String, Object> resolveAcControlInfo(Map<String, Object> meterInfo){
        logger.info("resolveAcControlInfo()");

        String meterSn =  (String) meterInfo.get("meter_sn");
        if(meterSn == null){
            logger.warning("Meter SN is null");
            return Map.of("error", "Meter SN is null");
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
            topicSub = "evs2/nus/vh/" + gw + "/" + meterSn;
            pmaPath = pagMqttAgentVhPath;
        } else if("nus_pgpr".equalsIgnoreCase(siteTag)){
            String gw = "gw1";
            String site = "pgpr";

            if("6".equals(block)){
                if(pgpr6AdhocMeter.contains(meterSn)){
                    block = "5";
                }
            }

            String topicInfix = block != null && !block.isEmpty() ? site + block : site;
            topicPub = "evs2/nus/" + topicInfix + "/" + gw;
            topicSub = "evs2/nus/" + topicInfix + "/" + gw + "/" + meterSn;
            pmaPath = pagMqttAgentPgprPath;
        } else {
            return Map.of("error", "Unsupported site tag: " + siteTag);
        }

        return Map.of("topic_pub", topicPub, "topic_sub", topicSub, "pma_path", pmaPath);
    }
}
