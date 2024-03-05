package com.pabu5h.evs2.evs2helper.report;

import com.pabu5h.evs2.dto.ItemIdTypeEnum;
import com.pabu5h.evs2.dto.ItemTypeEnum;
import com.pabu5h.evs2.evs2helper.locale.LocalHelper;
import com.pabu5h.evs2.evs2helper.scope.ScopeHelper;
import com.pabu5h.evs2.oqghelper.OqgHelper;
import com.pabu5h.evs2.oqghelper.QueryHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.logging.Logger;

@Component
public class TenantUsageProcessor {
    Logger logger = Logger.getLogger(TenantUsageProcessor.class.getName());

    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private QueryHelper queryHelper;
    @Autowired
    private ScopeHelper scopeHelper;
    @Autowired
    private LocalHelper localHelper;
    @Autowired
//    private MeterUsageProcessorLocal meterUsageProcessor;
    private MeterUsageProcessor meterUsageProcessor;

    public Map<String, Object> getListUsageSummary(Map<String, String> request) {
        logger.info("process getListUsageSummary");

        String projectScope = request.get("project_scope");
        String siteScope = request.get("site_scope");
        String meterSelectSql = request.get("id_select_query");
        String startDatetimeStr = request.get("start_datetime");
        String endDatetimeStr = request.get("end_datetime");
        String itemIdTypeStr = request.get("item_id_type");
        ItemIdTypeEnum itemIdType = ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
        String isMonthlyStr = request.get("is_monthly");
        boolean isMonthly = false;
        if (isMonthlyStr != null && !isMonthlyStr.isEmpty()) {
            isMonthly = Boolean.parseBoolean(isMonthlyStr);
        }

        String itemTypeStr = request.get("item_type");
        ItemTypeEnum itemTypeEnum = null;
        if(itemTypeStr != null) {
            itemTypeEnum = ItemTypeEnum.valueOf(itemTypeStr.toUpperCase());
        }
        if(itemTypeEnum == null){
            return Collections.singletonMap("error", "Invalid request");
        }
        String meterTypeStr = request.get("meter_type");
        ItemTypeEnum meterTypeEnum = null;
        if(meterTypeStr != null) {
            meterTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }
        if(meterTypeEnum == null){
            return Collections.singletonMap("error", "Invalid request");
        }

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig(projectScope, itemIdTypeStr);
        String targetTableName = (String) itemConfig.get("targetTableName");
        String tenantTableName = (String) itemConfig.get("tenantTableName");
        String targetReadingTableName = (String) itemConfig.get("targetReadingTableName");
        String targetGroupTargetTableName = (String) itemConfig.get("targetGroupTargetTableName");
        String tenantTargetGroupTableName = (String) itemConfig.get("tenantTargetGroupTableName");
        String itemIdColName = "tenant_name";
//        String itemIdColName = (String) itemConfig.get("itemIdColName");
//        String itemSnColName = (String) itemConfig.get("itemSnColName");
        String itemNameColName = (String) itemConfig.get("itemNameColName");
//        String itemAltName = (String) itemConfig.get("itemAltNameColName");
//        String panelTagColName = (String) itemConfig.get("panelTagColName");
        String timeKey =(String) itemConfig.get("timeKey");
        String valKey = (String) itemConfig.get("valKey");

        String sortBy = request.get("sort_by");
        String sortOrder = request.get("sort_order");

        Map<String, Object> result = new HashMap<>();

        int limit = Integer.parseInt(request.getOrDefault("max_rows_per_page", "20"));
        int page = Integer.parseInt(request.getOrDefault("current_page", "1"));
        int offset = (page - 1) * limit;
        //get count if it's the first page
        if (offset == 0) {
            String getCountQuery = meterSelectSql.replace("SELECT " + itemIdColName, "SELECT count(*)");
//            String getCountQuery = "SELECT count(*) FROM " + targetTableName;
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2(getCountQuery, true);
                int count = Integer.parseInt((String) resp.getFirst().get("count"));
                result.put("total_count", count);
            } catch (Exception e) {
                logger.severe(e.getMessage());
                return null;
            }
        }

        Map<String, Object> sort = new HashMap<>();
        if (sortBy != null && !sortBy.isEmpty() && sortOrder != null && !sortOrder.isEmpty()) {
            sort.put("sort_by", sortBy);
            sort.put("sort_order", sortOrder);
        }

        String fromSql = "SELECT id, " + itemIdColName + ", tenant_label, location_tag " +
                meterSelectSql.substring(meterSelectSql.indexOf(" FROM"));

        String meterSelectSql2 = fromSql + " ORDER BY " + itemIdColName + " LIMIT " + limit + " OFFSET " + offset;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(meterSelectSql2, true);
        } catch (Exception e) {
            logger.info("oqgHelper error: " + e.getMessage());
            return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
        }
        if (resp == null) {
            logger.info("oqgHelper error: resp is null");
            return Collections.singletonMap("error", "oqgHelper error: resp is null");
        }
        if (resp.isEmpty()) {
            logger.info("no meter found");
            return Collections.singletonMap("info", "no meter found");
        }

        List<Map<String, Object>> tenantUsageList = new ArrayList<>();
        for (Map<String, Object> tenantMap : resp) {
            Map<String, Object> requestTenant = new HashMap<>();
            requestTenant.put("scope_str", projectScope);
            requestTenant.put("item_id_type", "name");
            requestTenant.put("item_index", tenantMap.get("id"));
            requestTenant.put("get_full_info", "true");
            requestTenant.put("item_type", itemTypeEnum.name());
            requestTenant.put("target_group_target_table_name", targetGroupTargetTableName);
            requestTenant.put("tenant_target_group_table_name", tenantTargetGroupTableName);
            requestTenant.put("meter_type", meterTypeEnum.name());

            Map<String, Object> tenantMeters = queryHelper.getTenantMeterGroups(requestTenant);

            Map<String, Object> tenantResult = new HashMap<>();

            tenantResult.put("id", tenantMap.get("id"));
            tenantResult.put("tenant_name", tenantMap.get("tenant_name"));
            tenantResult.put("tenant_label", tenantMap.get("tenant_label"));

            //list of meter groups
            List<Map<String, Object>> meterGroups = (List<Map<String, Object>>) tenantMeters.get("group_full_info");

            List<Map<String, Object>> groupUsageList = new ArrayList<>();
            for (Map<String, Object> meterGroup : meterGroups) {
                String meterType = (String) meterGroup.get("meter_type");
                String groupName = (String) meterGroup.get("group_name");
                String label = (String) meterGroup.get("group_label");
                List<Map<String, Object>> meterList = (List<Map<String, Object>>) meterGroup.get("group_meter_list");
                Map<String, String> requestMeterListUsage = new HashMap<>();
                requestMeterListUsage.put("project_scope", projectScope);
                requestMeterListUsage.put("site_scope", siteScope);
                requestMeterListUsage.put("item_type", itemTypeEnum.name());
                requestMeterListUsage.put("item_id_list", meterList.toString());
                requestMeterListUsage.put("item_id_type", ItemIdTypeEnum.ID.name());
                requestMeterListUsage.put("start_datetime", startDatetimeStr);
                requestMeterListUsage.put("end_datetime", endDatetimeStr);
                requestMeterListUsage.put("is_monthly", String.valueOf(isMonthly));
                requestMeterListUsage.put("sort_by", "meter_sn");
                requestMeterListUsage.put("sort_order", "asc");
                requestMeterListUsage.put("max_rows_per_page", "1000");
                requestMeterListUsage.put("current_page", "1");

                Map<String, Object> usageResult = meterUsageProcessor.getMeterListUsageSummary(requestMeterListUsage, meterList);

                // handle percentage
                List<Map<String, Object>> meterListUsageSummary = (List<Map<String, Object>>) usageResult.get("meter_list_usage_summary");
                if (meterListUsageSummary == null) {
                    logger.info("no meter usage found");
                    continue;
                }
                for(Map<String, Object> meterUsage : meterListUsageSummary) {
                    //find percentage from meter list
                    String meterId = (String) meterUsage.get(itemNameColName);
                    Object percentage = meterList.stream()
                            .filter(m -> m.get(itemNameColName).equals(meterId))
                            .findFirst()
                            .map(m -> m.get("percentage"))
                            .orElse(null);

                    double percentageDouble = 100D;
                    if(percentage == null) {
                        logger.info("no matching meter found");
                    }else{
                        try {
//                            percentageDouble = Double.parseDouble((String) percentage);
                            percentageDouble = (Double) percentage;
                        } catch (NumberFormatException e) {
                            logger.info("percentage is not a number");
                        }
                    }
                    meterUsage.put("percentage", percentageDouble);
                }

                groupUsageList.add(Map.of(
                    "meter_group_name", groupName,
                    "meter_group_label", label,
                    "meter_type", meterType,
                    "meter_group_usage_summary", usageResult
                ));

            }
            tenantResult.put("tenant_usage_summary", groupUsageList);
            tenantUsageList.add(tenantResult);
        }

        return Collections.singletonMap("tenant_list_usage_summary", tenantUsageList);
    }

}
