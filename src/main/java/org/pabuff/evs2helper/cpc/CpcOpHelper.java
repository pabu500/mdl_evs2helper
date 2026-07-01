package org.pabuff.evs2helper.cpc;

import org.pabuff.oqghelper.OqgHelper;
import org.pabuff.utils.DateTimeUtil;
import org.pabuff.utils.SqlUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class CpcOpHelper {
    Logger logger = Logger.getLogger(CpcOpHelper.class.getName());

    @Autowired
    private OqgHelper oqgHelper;

    public Map<String, Object> postMeterServiceTarget(String meterSnStr, double refBal, String opTargetStr, String opTableName, boolean isReset){
//        String tableName = "meter_rls_op_test";
//        String tableName = "meter_rls_op";
        if(opTableName==null || opTableName.isEmpty()){
            opTableName = "cpc_op";
        }

        logger.info( "Posting: " + meterSnStr + " opTarget: " + opTargetStr + " with refBal: "+refBal);

        String sqlCheck = "SELECT current_status FROM " + opTableName + " WHERE meter_sn = '" + meterSnStr + "'";
        List<Map<String, Object>> resp = new ArrayList<>();
        try {
            resp = oqgHelper.OqgR2x(sqlCheck,"OPS", true);
        } catch (Exception e) {
            logger.info("Error getting meterRlsOp");
            return Map.of("error", e.getMessage());
        }
        boolean meterIsInCpcOpTable = !resp.isEmpty();

        // if not reset, check if meter is already in the target status
        // if is reset, skip this check, and always update the status to want_on or want_off to force the meter cpc
        if(!isReset) {
            if (meterIsInCpcOpTable) {
                String opStateStr = (String) resp.getFirst().get("current_status");
                opStateStr = opStateStr == null ? "" : opStateStr;
                if (opStateStr.equalsIgnoreCase(opTargetStr)) {
                    if (opStateStr.equalsIgnoreCase("off")) {
                        String message = meterSnStr + " already in target status: " + opTargetStr;
                        logger.info(message);
                        return Map.of("result", "success", "message", "meter already in target status");
                    }
                }
            }
        }

        String targetTimeStr = DateTimeUtil.getSgNowStr();
        String opStatusStr = opTargetStr.equalsIgnoreCase("on") ? "want_on" : "want_off";
        Map<String, Object> content = Map.of(
                "op_target", opTargetStr,
                "current_status", opStatusStr,
                "rcv_timestamp", targetTimeStr);
        Map<String, Object> sqlMap = Map.of("table", opTableName,
                "content", content,
                "target_key", "meter_sn",
                "target_value", meterSnStr);
        Map<String, String> sqlResult = SqlUtil.makeUpdateSql(sqlMap);
        if(! meterIsInCpcOpTable){
            Map<String, Object> content2 = new HashMap<>();
            content2.put("meter_sn", meterSnStr);
            content2.putAll(content);
            sqlMap = Map.of("table", opTableName,
                    "content", content2);
            sqlResult = SqlUtil.makeInsertSql(sqlMap);
        }
        String sql = sqlResult.get("sql");
        try {
            oqgHelper.OqgIU(sql);
        } catch (Exception e) {
            logger.info("Error updating meterRlsOp");
            return Map.of("error", e.getMessage());
        }
        return Map.of("result", "success");
    }
}
