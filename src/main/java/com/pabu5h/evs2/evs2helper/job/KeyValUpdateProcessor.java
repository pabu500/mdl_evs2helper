package com.pabu5h.evs2.evs2helper.job;

import com.pabu5h.evs2.dto.ItemIdTypeEnum;
import com.pabu5h.evs2.dto.ItemTypeEnum;
import com.pabu5h.evs2.evs2helper.cache.DataAgent;
import com.pabu5h.evs2.evs2helper.email.SystemNotifier;
import com.pabu5h.evs2.evs2helper.event.OpResultEvent;
import com.pabu5h.evs2.evs2helper.event.OpResultPublisher;
import com.pabu5h.evs2.evs2helper.report.ReportHelper;
import com.pabu5h.evs2.evs2helper.scope.ScopeHelper;
import com.pabu5h.evs2.oqghelper.OqgHelper;
import com.pabu5h.evs2.oqghelper.QueryHelper;
import com.xt.utils.DateTimeUtil;
import com.xt.utils.SqlUtil;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.logging.Logger;

@Service
@Getter
public class KeyValUpdateProcessor {
    private static final Logger logger = Logger.getLogger(KeyValUpdateProcessor.class.getName());
    @Autowired
    OqgHelper oqgHelper;
    @Autowired
    private QueryHelper queryHelper;
    @Autowired
    OpResultPublisher meterOpResultPublisher;
    @Autowired
    private DataAgent dataAgent;
    @Autowired
    SystemNotifier systemNotifier;
    @Autowired
    ScopeHelper scopeHelper;
    @Autowired
    private ReportHelper reportHelper;

//    private final Map<String, String> meterInfo = new ConcurrentHashMap<>();

    public Map<String, Object> getOpList(String tableName, String keyName, List<String> meterSns) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (String meterSn : meterSns) {
            String sql = "SELECT meter_sn, " + keyName +
                    " from " + tableName + " WHERE meter_sn = '" + meterSn + "'";
            try {
                List<Map<String, Object>> resp = oqgHelper.OqgR(sql);
                if(resp.isEmpty()){
                    continue;
                }
                result.add(resp.getFirst());
            } catch (Exception e) {
                logger.info("Error while getting" + keyName + " for meter: " + meterSn);
                systemNotifier.sendException("ORE Alert", KeyValUpdateProcessor.class.getName(), e.getMessage());
            }
        }
        return Map.of("conc_list", result);
    }

    public Map<String, Object> doOpSingleKeyValUpdate(
            String opName, String scopeStr,
            Map<String, Object> request,
            List<Map<String, Object>> opList,
            boolean isScheduledJobMode) {

        String meterTypeStr = (String) request.get("item_type");
        ItemTypeEnum itemTypeEnum = ItemTypeEnum.METER;
        if(meterTypeStr != null) {
            itemTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }
//        Map<String, Object> scopeConfig = scopeHelper.getItemTypeConfig(scopeStr, "");
//
//        ItemTypeEnum itemTypeEnum = ItemTypeEnum.valueOf((String) scopeConfig.get("itemTypeEnum"));

//        String opName = (String) request.get("op_name");
        String keyName = (String) request.get("key_name");

        String itemTableName = "meter";
        String itemSnKey = "meter_sn";
        String itemNameKey = "meter_displayname";
        switch (itemTypeEnum) {
            case METER-> {
                itemTableName = "meter";
                itemSnKey = "meter_sn";
                itemNameKey = "meter_displayname";
            }
            case METER_3P-> {
                itemTableName = "meter_3p";
                itemSnKey = "meter_sn";
                itemNameKey = "meter_id";
            }
            case METER_IWOW-> {
                itemTableName = "meter_iwow";
                itemSnKey = "item_sn";
                itemNameKey = "item_name";
            }
            case TENANT -> {
                itemTableName = "tenant";
                itemSnKey = "tenant_label";
                itemNameKey = "tenant_name";
            }
            case USER -> {
                itemTableName = "evs2_user";
                itemSnKey = "id";
                itemNameKey = "username";
            }
            case CONCENTRATOR -> {
                itemTableName = "concentrator";
//                itemSnKey = "concentrator_sn";
                itemNameKey = "concentrator_id";
            }
            case CONCENTRATOR_TARIFF -> {
                itemTableName = "concentrator_tariff";
                itemSnKey = "concentrator_id";
//                itemNameKey = "tariff_price";
            }

            default -> {
                return Map.of("error", "item_type not supported");
            }
        }

        String itemIdColName = itemSnKey;
        if(opName.contains("replacement")) {
            itemIdColName = itemNameKey;
        }

//        String itemIdTypeStr = (String) request.get("item_id_type");
//        ItemIdTypeEnum itemIdTypeEnum = null;
//        if(itemIdTypeStr != null && !itemIdTypeStr.isBlank()) {
//            itemIdTypeEnum = ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
//        }

        for( Map<String, Object> item : opList) {
            if(item.get("error") != null) {
                continue;
            }
            if(item.get("status") != null && ((String)item.get("status")).contains("error")) {
                continue;
            }
            if(item.get("checked") == null || !(boolean) item.get("checked")) {
                continue;
            }

            String displayOpName = opName;
            if(opName.contains(".")) {
                displayOpName = opName.split("\\.")[1];
            }
            String op = item.get("op") == null ? displayOpName : (String) item.get("op");

            String itemSn = (String) item.get(itemSnKey);
            String itemIdValue = itemSn;

            if(itemTypeEnum == ItemTypeEnum.USER) {
                itemIdColName = itemNameKey;
                itemIdValue = (String) item.get(itemNameKey);
            }

            if(opName.contains("replacement")) {
                String meterDisplayname = (String) item.get(itemNameKey);
                if (meterDisplayname == null || meterDisplayname.isBlank()) {
                    logger.info("Error while doing " + opName + " op for item: " + meterDisplayname);
                    continue;
                }
                itemIdValue = meterDisplayname;
            }

            if(itemIdValue == null || itemIdValue.isBlank()) {
                String itemName = (String) item.get(itemNameKey);
                if(itemName == null || itemName.isBlank()) {
                    logger.info("Error while doing " + opName + " op for item: " + itemName);
                    continue;
                }
//                itemSn = meterInfo.get(itemName);
//                Map<String, Object> meterSnResult = dataAgent.getMeterSnFromDisplayname(itemName);
//                itemSn = (String) meterSnResult.get(itemSnKey);

                //query db instead of cache
                if(itemTypeEnum == ItemTypeEnum.METER) {
                    itemSn = queryHelper.getMeterSnFromMeterDisplayname(itemName);

                    if (itemSn == null || itemSn.isBlank()) {
                        logger.info("Error while doing " + opName + " op for item: " + itemName);
                        item.put("error", Map.of("status", "Meter not found"));
                        item.put("prev_status", item.get("status"));
                        item.put("status", op + " error");
                        continue;
                    }
                }
            }

            String newTargetKey = "new_" + keyName;

            logger.info("Doing " + opName + " op for item: " + itemSn);
            boolean mock = false;
            if(mock){
                //mock
                try {
                    Thread.sleep(600);
                    if(item.get("meter_displayname").equals("10013014")) {
                        throw new Exception("Meter displayname error");
                    }
                    item.put("prev_status", item.get("status"));
                    item.put("status", op + " success");
                    item.put(newTargetKey, item.get(keyName));
                }catch (Exception e){
                    item.put("error", Map.of("status", e.getMessage()));
                    item.put("prev_status", item.get("status"));
                    item.put("status", op + " error");
                    item.put("checked", false);
                }
            }else {
                //live
                String localNowStr = DateTimeUtil.getSgNowStr();

                String val = (String) item.get(keyName);
                //if val is a number, do not quote it
                // adding '' is handled by SqlUtil.makeUpdateSql
//                if(val.matches("-?\\d+(\\.\\d+)?")){
//                    val = val;
//                }else{
//                    val = "'" + val + "'";
//                }
                Map<String, Object> content = new HashMap<>();
                content.put(keyName, val);

                if(opName.contains("replacement")) {
                    content.put("commission_timestamp", localNowStr);
                }

                if(opName.contains("setsite")){
                    String siteScope = (String) item.get(keyName);
                    siteScope = siteScope.toLowerCase();
                    Map<String, Object> resultProj = queryHelper.getProjectScopeFromSiteScope(siteScope);
                    if(resultProj.containsKey("project_scope")){
                        String projectScope = (String) resultProj.get("project_scope");
                        content.put("scope_str", projectScope);

                        //update user as well
                        if(itemTypeEnum == ItemTypeEnum.METER){
                            String meterDisplayname = (String) item.get(itemNameKey);
                            if(meterDisplayname == null || meterDisplayname.isBlank()) {
                                meterDisplayname = queryHelper.getMeterDisplaynameFromSn(itemSn);
                            }
                            if(meterDisplayname.isBlank()) {
                                logger.info("Error while doing " + opName + " op for item: " + itemSn);
                                item.put("error", Map.of("status", "Meter displayname not found"));
                                item.put("prev_status", item.get("status"));
                                item.put("status", op + " error");
                                item.put("checked", false);
                                continue;
                            }

                            Map<String, Object> result = queryHelper.setUserScope(meterDisplayname, projectScope);
                            if(result.containsKey("error")) {
                                logger.info("Error while doing " + opName + " op for item: " + itemSn);
                                item.put("error", Map.of("status", result.get("error")));
                                item.put("prev_status", item.get("status"));
                                item.put("status", op + " error");
                                item.put("checked", false);
                                continue;
                            }
                        }
                    }else {
                        logger.info("Error while doing " + opName + " op for item: " + itemSn);
                        item.put("error", Map.of("status", "Project scope not found"));
                        item.put("prev_status", item.get("status"));
                        item.put("status", op + " error");
                        item.put("checked", false);
                        continue;
                    }
                }
//                String sql = "UPDATE " + tableName + " SET " + keyName + " = " + val +
//                        " WHERE meter_sn = '" + itemSn + "'";

                Map<String, String> sqlResult = SqlUtil.makeUpdateSql(
                        Map.of(
                                "table", itemTableName,
                                "target_key", itemIdColName,
                                "target_value", itemIdValue,
                                "content", content));
                String sql = sqlResult.get("sql");
                try {
                    Map<String, Object> resp;
                    resp = oqgHelper.OqgIU(sql);
                    if (resp.containsKey("error")) {
                        logger.info("Error while doing " + op + " for item: " + itemSn);
                        item.put("error", Map.of("status", resp.get("error")));
                        item.put("prev_status", item.get("status"));
                        item.put("status", op + " error");
                        item.put("checked", false);
//                            continue;
                    }else{
                        item.put("prev_status", item.get("status"));
                        item.put("status", op + " success");
//                        long concId = queryHelper.getConcentratorIdFromMeterSn(itemSn);
                        List<Map<String, Object>> newValResp;
                        String newValSql = "SELECT " + keyName + " from " + itemTableName +
                                " WHERE " + itemIdColName + " = '" + itemIdValue + "'";
                        try {
                            newValResp = oqgHelper.OqgR(newValSql);
                            String newVal = (String) newValResp.getFirst().get(keyName);
                            item.put(newTargetKey, newVal);
                        }catch (Exception e) {
                            logger.info("Error while getting new " + keyName + " for item: " + itemSn);
                            item.put("error", Map.of("status", e.getMessage()));
                            item.put("prev_status", item.get("status"));
                            item.put("status", op + " error");
                            item.put("checked", false);
                        }
                    }
                } catch (Exception ex) {
                    logger.info("Error while doing " + op + " for item: " + itemSn);
                    item.put("error", Map.of("status", ex.getMessage()));
                    item.put("prev_status", item.get("status"));
                    item.put("status", op + " error");
                    item.put("checked", false);
//                        continue;
                }

            }
            if(!isScheduledJobMode) {
                meterOpResultPublisher.publishEvent(OpResultEvent.builder()
                        .updatedBatchList(opList)
                        .meterOp(/*"do_op_" + */opName)
                        .build());
            }
        }

        if(isScheduledJobMode) {
            List<LinkedHashMap<String, Object>> report = new ArrayList<>();
            for (Map<String, Object> item : opList) {
                LinkedHashMap<String, Object> rec = new LinkedHashMap<>();
                for (String key : item.keySet()) {
                    rec.put(key, item.get(key));
                }
                report.add(rec);
            }

            LinkedHashMap<String, Integer> headerMap = new LinkedHashMap<>();
            for (String key : opList.getFirst().keySet()) {
                headerMap.put(key, 5000);
            }

            Map<String, Object> result = reportHelper.genReportExcel(opName, report, headerMap, "result");
            return result;
        }else{
            return Map.of("list_op_result", opList);
        }
    }

    public Map<String, Object> doOpMultiKeyValUpdate(Map<String, Object> request,
                                                     List<Map<String, Object>> opList) {

        String meterTypeStr = (String) request.get("item_type");
        ItemTypeEnum meterTypeEnum = ItemTypeEnum.METER;
        if(meterTypeStr != null) {
            meterTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }

        String itemTableName = "meter";
        String itemSnKey = "meter_sn";
        String itemNameKey = "meter_displayname";
        switch (meterTypeEnum) {
            case METER-> {
                itemTableName = "meter";
                itemSnKey = "meter_sn";
                itemNameKey = "meter_displayname";
            }
            case METER_3P-> {
                itemTableName = "meter_3p";
                itemSnKey = "meter_sn";
                itemNameKey = "meter_id";
            }
            case METER_IWOW-> {
                itemTableName = "meter_iwow";
                itemSnKey = "item_sn";
                itemNameKey = "item_name";
            }
            case TENANT -> {
                itemTableName = "tenant";
                itemSnKey = "tenant_label";
                itemNameKey = "tenant_name";
            }
            case METER_GROUP -> {
                itemTableName = "meter_group";
                itemSnKey = "id";
                itemNameKey = "name";
            }
            default -> {
                return Map.of("error", "item_type not supported");
            }
        }

        String opName = (String) request.get("op_name");

        String itemIdTypeStr = (String) request.get("item_id_type");
        ItemIdTypeEnum itemIdTypeEnum = null;
        if(itemIdTypeStr != null && !itemIdTypeStr.isBlank()) {
            itemIdTypeEnum = ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
        }

        String displayOpName = opName;
        if(opName.contains(".")) {
            displayOpName = opName.split("\\.")[1];
        }
        for( Map<String, Object> item : opList) {
            if(item.get("error") != null) {
                continue;
            }
            if(item.get("status") != null && ((String)item.get("status")).contains("error")) {
                continue;
            }
            if(item.get("checked") == null || !(boolean) item.get("checked")) {
                continue;
            }

            String itemSn = (String) item.get(itemSnKey);
            String itemName = (String) item.get(itemNameKey);

            String targetKey = itemSnKey;
            String targetValue = itemSn;

            String op = item.get("op") == null ? displayOpName : (String) item.get("op");

            if(itemIdTypeEnum == null) {
                if (((itemName == null || itemName.isBlank()) || (itemSn == null || itemSn.isBlank()))) {
                    logger.info("Error while doing " + opName + " op for item");
                    continue;
                }
            }else {
                if(itemIdTypeEnum == ItemIdTypeEnum.SN) {
                    if ((itemSn == null || itemSn.isBlank())) {
                        logger.info("Error while doing " + opName + " op for item");
                        continue;
                    }
                }else if(itemIdTypeEnum == ItemIdTypeEnum.NAME) {
                    if ((itemName == null || itemName.isBlank())) {
                        logger.info("Error while doing " + opName + " op for item");
                        continue;
                    }
                    targetKey = itemNameKey;
                    targetValue = itemName;
                }
            }

            logger.info("Doing " + opName + " for item: " + itemSn);
            boolean mock = false;
            if(mock){
                //mock
                try {
                    Thread.sleep(600);
                    if(item.get(itemNameKey).equals("10013014")) {
                        throw new Exception("Meter displayname error");
                    }
                    item.put("prev_status", item.get("status"));
                    item.put("status", op + " success");
//                    item.put(newTargetKey, targetValue);
                }catch (Exception e){
                    item.put("error", Map.of("status", e.getMessage()));
                    item.put("prev_status", item.get("status"));
                    item.put("status", opName + " error");
                    item.put("checked", false);
                }
            }else {
                //live
                String localNowStr = DateTimeUtil.getSgNowStr();

                //sort thru the item map for key and non-empty value pairs to update
                Map<String, Object> content = new HashMap<>();
                for(Map.Entry<String, Object> entry : item.entrySet()) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
                        if (key.equals("item_name")){
                            continue;
                        }
                    }else if(meterTypeEnum == ItemTypeEnum.TENANT){
                        if (key.equals("tenant_name")){
                            continue;
                        }
                        if(key.equals("tenant_label") && (val == null || val.toString().isBlank())){
                            continue;
                        }
                        if(key.equals("type")){
                            String typeVal = (String) val;
                            String tenantName = (String) item.get("tenant_name");
                            String newTenantName = tenantName;
                            if(typeVal.equals("cw_nus_external") && tenantName.contains("-int-")){
                                newTenantName = tenantName.replace("-int-", "-ext-");
                            }else if(typeVal.equals("cw_nus_internal") && tenantName.contains("-ext-")){
                                newTenantName = tenantName.replace("-ext-", "-int-");
                            }
                            content.put("tenant_name", newTenantName);
                        }
                    }else if(opName.equals("replacement")) {
                        if (key.equals(itemNameKey)) {
                            continue;
                        }
                    }else{
                        if (key.equals(itemSnKey) || key.equals(itemNameKey)) {
                            continue;
                        }
                    }
                    if (key.equals("error") || key.equals("checked") || key.equals("status") || key.equals("prev_status")) {
                        continue;
                    }
                    if (val == null /*|| val.toString().isBlank()*/) {
                        continue;
                    }
                    content.put(key, val);
                    if(opName.equals("replacement")) {
                        content.put("commission_timestamp", localNowStr);
                    }
                }
                if(content.isEmpty()){
                    logger.info("Error while doing " + opName + " op for item: " + itemSn);
                    item.put("error", Map.of("status", "No content to update"));
                    item.put("prev_status", item.get("status"));
                    item.put("status", op + " error");
                    item.put("checked", false);
                    continue;
                }
                Map<String, String> sqlResult = SqlUtil.makeUpdateSql(
                                                Map.of(
                                                "table", itemTableName,
                                                "target_key", targetKey,
                                                "target_value", targetValue,
                                                "content", content));

                String sql = sqlResult.get("sql");
                try {
                    Map<String, Object> resp;
                    resp = oqgHelper.OqgIU(sql);
                    if (resp.containsKey("error")) {
                        logger.info("Error while doing " + op + " for item: " + itemSn);
                        item.put("error", Map.of("status", resp.get("error")));
                        item.put("prev_status", item.get("status"));
                        item.put("status", op + " error");
                        item.put("checked", false);
//                            continue;
                    }else{
                        item.put("prev_status", item.get("status"));
                        item.put("status", op + " success");
                    }
                } catch (Exception ex) {
                    logger.info("Error while doing " + op + " for item: " + itemSn);
                    item.put("error", Map.of("status", ex.getMessage()));
                    item.put("prev_status", item.get("status"));
                    item.put("status", op + " error");
                    item.put("checked", false);
//                        continue;
                }
            }
            meterOpResultPublisher.publishEvent(OpResultEvent.builder()
                    .updatedBatchList(opList)
                    .meterOp(/*"do_op_" + */opName)
                    .build());
        }
        return Map.of("list_op_result", opList);
    }
}
