package org.pabuff.evs2helper.meter_group;

import org.pabuff.dto.ItemIdTypeEnum;
import org.pabuff.dto.ItemTypeEnum;
import org.pabuff.evs2helper.meter_usage.MeterUsageProcessor;
import org.pabuff.evs2helper.scope.ScopeHelper;
import org.pabuff.oqghelper.OqgHelper;
import org.pabuff.oqghelper.QueryHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.logging.Logger;

@Component
public class MeterGroupUsageProcessor {
    Logger logger = Logger.getLogger(MeterGroupUsageProcessor.class.getName());

    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private QueryHelper queryHelper;
    @Autowired
    private ScopeHelper scopeHelper;
    @Autowired
    private MeterUsageProcessor meterUsageProcessor;

    public Map<String, Object> getListUsageSummary(Map<String, String> request) {
        logger.info("process getListUsageSummary");

        String projectScope = request.get("project_scope");
        String siteScope = request.get("site_scope");
        String meterSelectSql = request.get("id_select_query");
        String startDatetimeStr = request.get("start_datetime");
        String endDatetimeStr = request.get("end_datetime");
        String itemIdTypeStr = request.get("item_id_type");
        ItemIdTypeEnum itemIdTypeEnum = ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
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
        String getTrendingSnapshot = request.getOrDefault("get_trending_snapshot", "false");
        boolean getTrendingSnapshotBool = Boolean.parseBoolean(getTrendingSnapshot);

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig(projectScope, itemIdTypeStr);
        String targetTableName = (String) itemConfig.get("targetTableName");
        String tenantTableName = (String) itemConfig.get("tenantTableName");
        String targetReadingTableName = (String) itemConfig.get("targetReadingTableName");
        String targetGroupTargetTableName = (String) itemConfig.get("targetGroupTargetTableName");
        String itemIdColName = "name";
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

        String fromSql = "SELECT id, " + itemIdColName + ", label, meter_type " +
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
            logger.info("no meter group found");
            return Collections.singletonMap("info", "no meter found");
        }

        List<Map<String, Object>> meterGroupUsageList = new ArrayList<>();
        for (Map<String, Object> meterGroup : resp) {
            String meterType = (String) meterGroup.get("meter_type");
            String groupName = (String) meterGroup.get("name");
            String label = (String) meterGroup.get("label");
            Map<String, Object> requestMeterList = new HashMap<>();
            requestMeterList.put("scope_str", projectScope);
            requestMeterList.put("item_id_type", itemIdTypeEnum.name());
            requestMeterList.put("item_name", groupName);
            requestMeterList.put("label", label);
            requestMeterList.put("meter_type", meterTypeEnum.name());
            requestMeterList.put("item_index", meterGroup.get("id"));
            requestMeterList.put("item_type", itemTypeEnum.name());

            Map<String, Object> meterListResult = queryHelper.getGroupMeters(requestMeterList, targetGroupTargetTableName);
            List<Map<String, Object>> meterList = (List<Map<String, Object>>) meterListResult.get("group_meter_list");
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

            Map<String, Object> usageResult = meterUsageProcessor.getMeterListUsageSummary(requestMeterListUsage, meterList, null);

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

            Map<String, Object> groupUsage = new HashMap<>();
            groupUsage.put("meter_group_name", groupName);
            groupUsage.put("meter_group_label", label);
            groupUsage.put("meter_type", meterType);
            groupUsage.put("meter_group_usage_summary", usageResult);
            if (getTrendingSnapshotBool) {
                List<String> meterIdList = new ArrayList<>();
                for (Map<String, Object> meter : meterList) {
                    meterIdList.add((String) meter.get(itemNameColName));
                }
                Map<String, Object> trendingSnapshot = getMeterGroupTrendingSnapshot(
                        projectScope, siteScope,
                        meterTypeEnum.name(),
                        itemIdTypeEnum.name(),
                        //join meter id list
                        String.join(",", meterIdList),
                        groupName,
                        startDatetimeStr, endDatetimeStr,
                        isMonthly
                );
                if (trendingSnapshot.containsKey("error")) {
                    logger.severe("Error getting trending snapshot: " + trendingSnapshot.get("error"));
                }else {
                    groupUsage.put("meter_group_trending_snapshot", trendingSnapshot);
                }
            }
            meterGroupUsageList.add(groupUsage);
        }

        return Collections.singletonMap("meter_group_list_usage_summary", meterGroupUsageList);
    }

    public Map<String, Object> getMeterGroupTrendingSnapshot(String projectScope, String siteScope,
                                                             String meterTypeStr,
                                                             String itemIdTypeStr,
                                                             String meterList,
                                                             String meterGroupId,
                                                             String startDatetimeStr, String endDatetimeStr,
                                                             boolean isMonthly) {
        try {
            Map<String, String> consolidatedUsageHistoryRequest = new HashMap<>();
            consolidatedUsageHistoryRequest.put("target_interval", "month");
            consolidatedUsageHistoryRequest.put("num_of_intervals", "3");
            consolidatedUsageHistoryRequest.put("project_scope", projectScope);
            consolidatedUsageHistoryRequest.put("site_scope", siteScope);
            consolidatedUsageHistoryRequest.put("item_type", meterTypeStr);
            consolidatedUsageHistoryRequest.put("group_name", meterGroupId);
            consolidatedUsageHistoryRequest.put("item_id_type", itemIdTypeStr);
            consolidatedUsageHistoryRequest.put("item_id_list", meterList);
            consolidatedUsageHistoryRequest.put("start_datetime", startDatetimeStr);
            consolidatedUsageHistoryRequest.put("end_datetime", endDatetimeStr);
            consolidatedUsageHistoryRequest.put("is_monthly", String.valueOf(isMonthly));
            consolidatedUsageHistoryRequest.put("sort_by", "kwh_timestamp");
            consolidatedUsageHistoryRequest.put("sort_order", "desc");
            consolidatedUsageHistoryRequest.put("max_rows_per_page", "1");
            consolidatedUsageHistoryRequest.put("current_page", "1");

            Map<String, Object> trendingResult =
                    meterUsageProcessor.getMeterConsolidatedUsageHistory(consolidatedUsageHistoryRequest);

            return trendingResult;
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

}
