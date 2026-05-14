package org.pabuff.evs2helper.cpc;

import org.pabuff.oqghelper.OqgHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class CommandResolver {
    Logger logger = Logger.getLogger(CommandResolver.class.getName());

    @Autowired
    OqgHelper oqgHelper;

    @Deprecated
    public Map<String, Object> resolveCommand(String meterSn) {

        //resolve OFF
        String sql = "SELECT scope_str FROM meter WHERE meter_sn = '" + meterSn + "' ";
        List<Map<String, Object>> resp = new ArrayList<>();
        try {
            resp = oqgHelper.OqgR2x(sql, "OPS",true);
        } catch (Exception e) {
            return Map.of("error", e.getMessage());
        }

        if (resp.isEmpty()) {
            return Map.of("info", "meter not found");
        }
        String scopeStr = (String) resp.getFirst().get("scope_str");
        if(scopeStr == null || scopeStr.isEmpty()){
            return Map.of("error", "scope_str is empty");
        }

        if(scopeStr.contains("evs2_ntu")){
            return Map.of("on_off_command", CommandType.acLock.toString());
        }if(scopeStr.contains("nus_vh")){
            return Map.of("on_off_command", CommandType.acController.toString());
        } else{
            return Map.of("on_off_command", CommandType.rls.toString());
        }
    }

    public Map<String, Object> resolveCommandType(String meterSn) {

        //resolve OFF
        String sql = "SELECT site_tag FROM meter WHERE meter_sn = '" + meterSn + "' ";
        List<Map<String, Object>> resp = new ArrayList<>();
        try {
            resp = oqgHelper.OqgR2x(sql, "OPS",true);
        } catch (Exception e) {
            return Map.of("error", e.getMessage());
        }

        if (resp.isEmpty()) {
            return Map.of("info", "meter not found");
        }
        String siteTag = (String) resp.getFirst().get("site_tag");
        if(siteTag == null || siteTag.isEmpty()){
            return Map.of("error", "scope_str is empty");
        }

        if(siteTag.contains("ntu_mr")){
            return Map.of("command_type", CommandType.acLock);
        }if(siteTag.contains("nus_vh")){
            return Map.of("command_type", CommandType.acController);
        } else{
            return Map.of("command_type", CommandType.rls);
        }
    }

    public Map<String, Object> resolveCommandType2(Map<String, Object> meterInfo) {

        String siteTag = (String) meterInfo.get("site_tag");
        if(siteTag == null || siteTag.isEmpty()){
            return Map.of("error", "scope_str is empty");
        }
        String commType = (String) meterInfo.get("comm_type");
        String block = (String) meterInfo.get("mms_block");

        if("evs2_loop".equals(commType)){
            if("nus_ync".equals(siteTag) || "nus_utown".equals(siteTag)){
                return Map.of("command_type", CommandType.rlsEvs2Loop);
            }
        }
        if(siteTag.contains("ntu_mr")){
            return Map.of("command_type", CommandType.acLock);
        }
        if(siteTag.contains("nus_vh")){
            return Map.of("command_type", CommandType.acController);
        }
        if(siteTag.contains("nus_pgpr") && (block != null && !block.isEmpty())){
            if(!"13".equals(block) && !"14".equals(block)){
                return Map.of("command_type", CommandType.acController);
            }
        }

        return Map.of("command_type", CommandType.rls);

    }
}
