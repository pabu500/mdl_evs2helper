//package org.pabuff.evs2helper.project_ore;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.pabuff.dto.PagScopeTypeEnum;
//import org.pabuff.dto.StdErrorDto;
//import org.pabuff.oqghelper2.OqgHelper2;
//import org.pabuff.paghelper.locale.LocalHelper;
//import org.pabuff.utils.ApiCode;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;
//
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.logging.Logger;
//
//@Component
//public class OreProcessor {
//    Logger logger = Logger.getLogger(OreProcessor.class.getName());
//
////    @Value("${useProjectOreUrl}")
////    private String useProjectOreUrl;
//
//    // 3 values: dev, deploy-test, deploy
//    @Value("${service.address.type}")
//    private String serviceAddressType;
//
//    @Autowired
//    private OqgHelper2 oqgHelper;
//    @Autowired
//    private LocalHelper localHelper;
//
//    public interface GetProjectItemListTypeName {
//        Map<String, Object> run(String itemKindEnumStr, String itemTypeStr);
//    }
//
//    public interface GetFleetHealth {
//        Map<String, Object> run(Map<String, Object> request);
//    }
//
//    public interface GetMeterPeriodTotal {
//        Map<String, Object> run(Map<String, Object> request);
//    }
//
//
//    public Map<String, Object> getProjectInfo(Map<String, Object> request) {
//        logger.info("getProjectInfo");
//
//        String siteTableName = (String) request.get("site_table_name");
//        if (siteTableName == null || siteTableName.isEmpty()) {
//            return Map.of("error", "site_table_name is required");
//        }
//
//        String populatingBuildingList = (String) request.get("populate_building_list");
//        if ("true".equals(populatingBuildingList)) {
//            String buildingTableName = (String) request.get("building_table_name");
//            if (buildingTableName == null || buildingTableName.isEmpty()) {
//                return Map.of("error", "building_table_name is required");
//            }
//        }
//
//        String deviceInfoTypeTableName = (String) request.get("device_type_info_table_name");
//        if (deviceInfoTypeTableName == null || deviceInfoTypeTableName.isEmpty()) {
//            return Map.of("error", "device_type_info_table_name is required");
//        }
//
//        Map<String, Object> siteList = getSiteList(request);
//        if (siteList.get("error") != null) {
//            return Map.of("error", siteList.get("error"));
//        }
//        Map<String, Object> deviceTypeInfoList = getDeviceTypeInfoList(request);
//        if (deviceTypeInfoList.get("error") != null) {
//            return Map.of("error", deviceTypeInfoList.get("error"));
//        }
//        return Collections.singletonMap("project_info",
//                Map.of(
//                        "site_list", siteList.get("site_list"),
//                        "device_type_info_list", deviceTypeInfoList.get("device_type_info_list"))
//        );
//    }
//
//    public Map<String, Object> getSiteList(Map<String, Object> request) {
//        logger.info("getSiteList");
//
//        String siteTableName = (String) request.get("site_table_name");
//        if (siteTableName == null || siteTableName.isEmpty()) {
//            return Map.of("error", "site_table_name is required");
//        }
//
//        //default to not populate building list unless specified as 'true'
//        boolean populateBuilding_list = "true".equals(request.get("populate_building_list"));
//
//        String sql = "SELECT * FROM " + siteTableName;
//
//        List<Map<String, Object>> resp;
//        try {
//            resp = oqgHelper.OqgR2x(sql, "OPS", true);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return Map.of("error", e.getMessage());
//        }
//
//        if(populateBuilding_list){
//            for (Map<String, Object> site : resp) {
//                String siteId = (String) site.get("id");
//                Map<String, Object> requestBuilding = new HashMap<>();
//                requestBuilding.put("site_id", siteId);
//                requestBuilding.put("building_table_name", request.get("building_table_name"));
//                Map<String, Object> buildingList = getBuildingList(requestBuilding);
//                if (buildingList.get("error") != null) {
//                    return Map.of("error", buildingList.get("error"));
//                }
//                site.put("building_list", buildingList.get("building_list"));
//            }
//        }
//
//        return Map.of("site_list", resp);
//    }
//
//    public Map<String, Object> getBuildingList(Map<String, Object> request) {
//        logger.info("getBuildingList");
//
//        String buildingTableName = (String) request.get("building_table_name");
//        if (buildingTableName == null || buildingTableName.isEmpty()) {
//            return Map.of("error", "building_table_name is required");
//        }
//
//        String siteId = (String) request.get("site_id");
//        if (siteId == null || siteId.isEmpty()) {
//            return Map.of("error", "site_id is required");
//        }
//
//        String sql = "SELECT * FROM " + buildingTableName + " WHERE site_id = '" + siteId + "'";
//
//        List<Map<String, Object>> resp;
//        try {
//            resp = oqgHelper.OqgR2x(sql, "OPS", true);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return Map.of("error", e.getMessage());
//        }
//        return Map.of("building_list", resp);
//    }
//
//    public Map<String, Object> getDeviceTypeInfoList(Map<String, Object> request) {
//        logger.info("getDeviceTypeInfoList");
//
//        String deviceInfoTypeTableName = (String) request.get("device_type_info_table_name");
//        if (deviceInfoTypeTableName == null || deviceInfoTypeTableName.isEmpty()) {
//            return Map.of("error", "device_type_info_table_name is required");
//        }
//
//        String sql = "SELECT * FROM " + deviceInfoTypeTableName;
//
//        List<Map<String, Object>> resp;
//        try {
//            resp = oqgHelper.OqgR2x(sql, "OPS", true);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return Map.of("error", e.getMessage());
//        }
//        return Map.of("device_type_info_list", resp);
//    }
//
//    public Map<String, Object> getScopeUsageList(Map<String, Object> request, GetMeterPeriodTotal getMeterPeriodTotal) {
//        logger.info("getScopeUsageList");
//
//        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
//        if (scope == null) {
//            return Map.of("error", "scope is required");
//        }
//
//        String topStr = (String) request.get("top");
//        if (topStr == null) {
//            logger.severe("top is required");
//            return Map.of("error", "top is required");
//        }
//
//        List<Map<String, Object>> scopeInfoList = (List<Map<String, Object>>) request.get("scope_info_list");
//        if (scopeInfoList == null) {
//            logger.severe("scope_info_list is required");
//            return Map.of("error", "scope_info_list is required");
//        }
//        String childrenScopeTypeStr = (String) request.get("children_scope_type");
//        if (childrenScopeTypeStr == null) {
//            logger.severe("children_scope_type is required");
//            return Map.of("error", "children_scope_type is required");
//        }
//        PagScopeTypeEnum childrenScopeType;
//        try {
//            childrenScopeType = PagScopeTypeEnum.valueOf(childrenScopeTypeStr);
//        } catch (IllegalArgumentException e) {
//            logger.severe("Invalid list_scope_type");
//            return Map.of("error", "Invalid list_scope_type");
//        }
//
//        StringBuilder sqlUsageList = new StringBuilder();
//        String sel = "";
//        int i = 0;
//        for (Map<String, Object> scopeInfo : scopeInfoList) {
//            StringBuilder sqlUsage = new StringBuilder();
//
//            Map<String, Object> itemScope = new HashMap<>();
//            String itemId = (String) scopeInfo.get("id");
//            String itemName = (String) scopeInfo.get("name");
//            String itemLabel = (String) scopeInfo.get("label");
//
//            itemScope.put("project_name", scope.get("project_name"));
//            itemScope.put("project_id", scope.get("project_id"));
//
//            switch (childrenScopeType) {
//                case location -> {
//                    itemScope.put("location_id", itemId);
//                    itemScope.put("location_name", itemName);
//                }
//                case locationGroup -> {
//                    itemScope.put("location_group_id", itemId);
//                    itemScope.put("location_group_name", itemName);
//                }
//                case building -> {
//                    itemScope.put("building_id", itemId);
//                    itemScope.put("building_name", itemName);
//                }
//                case site -> {
//                    itemScope.put("site_id", itemId);
//                    itemScope.put("site_name", itemName);
//                }
//                case siteGroup -> {
//                    itemScope.put("site_group_id", itemId);
//                    itemScope.put("site_group_name", itemName);
//                }
//                default -> {
//                    logger.severe("Invalid list_scope_type");
//                    return Map.of("error", "Invalid list_scope_type");
//                }
//            }
//
//            Map<String, Object> usageRequest = new HashMap<>();
//            usageRequest.put("scope", itemScope);
//
//            // add from and to timestamp
//            String lookbackType = (String) request.get("lookback_type");
//
//            LocalDateTime localNow = localHelper.getLocalNow();
//            LocalDateTime toDateTime = localNow;
//
//            Duration duration = Duration.ofDays(1);
//            if("last_24h".equalsIgnoreCase(lookbackType)) {
//                duration = Duration.ofDays(1);
//            }else if("last_7d".equalsIgnoreCase(lookbackType)) {
//                duration = Duration.ofDays(7);
//                // align toDateTime to 00:00:00
//                toDateTime = localNow.withHour(0).withMinute(0).withSecond(0);
//            }else if("last_14d".equalsIgnoreCase(lookbackType)) {
//                duration = Duration.ofDays(14);
//                // align toDateTime to 00:00:00
//                toDateTime = localNow.withHour(0).withMinute(0).withSecond(0);
//            }else if("mtd".equalsIgnoreCase(lookbackType)) {
//                //current month
//                LocalDateTime firstDayOfMonth = localNow.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
//                // align toDateTime to 00:00:00
//                toDateTime = localNow.withHour(0).withMinute(0).withSecond(0);
//                duration = Duration.between(firstDayOfMonth, toDateTime);
//            }
//            LocalDateTime fromDateTime = localNow.minus(duration);
//
//            usageRequest.put("from_timestamp", fromDateTime.toString());
//            usageRequest.put("to_timestamp", toDateTime.toString());
//            usageRequest.put("get_period_sum_diff_query_only", "true");
//
//            String mainSubStr = (String) request.get("main_sub_str");
//            usageRequest.put("main_sub_str", mainSubStr);
//
//            Map<String, Object> totalResult = getMeterPeriodTotal.run(usageRequest);
//            if (totalResult.get("error") != null) {
//                logger.severe((String) totalResult.get("error"));
//                continue;
//            }
//            Map<String, Object> meterPeriodTotal = (Map<String, Object>) totalResult.get("meter_period_total");
//            String sql = (String) meterPeriodTotal.get("sql");
//            if(sql == null || sql.isEmpty()) {
//                logger.severe("sql not found in meterPeriodTotal");
//                return Map.of("error", "sql not found in meterPeriodTotal");
//            }
//            if(sel.isEmpty()) {
//                sel = (String) meterPeriodTotal.get("sel");
//                if(sel == null || sel.isEmpty()) {
//                    logger.severe("sel not found in meterPeriodTotal");
//                    return Map.of("error", "sel not found in meterPeriodTotal");
//                }
//            }
//
//            sqlUsage.append(sql);
//            // replace sel with sel + id, name, label
//            sqlUsage.replace(sqlUsage.indexOf(sel), sqlUsage.indexOf(sel) + sel.length(),
//                    sel + ", '" + itemId + "' AS id, '" + itemName + "' AS name, '" + itemLabel + "' AS label");
//
//            sqlUsageList.append(sqlUsage);
//
//            i++;
//            // append if not the last item
//            if(i < scopeInfoList.size()) {
//                sqlUsageList.append(" UNION ALL ");
//            }
//
////            Map<String, Object> usageInfo = new HashMap<>();
////            usageInfo.put("id", scopeInfo.get("id"));
////            usageInfo.put("name", scopeInfo.get("name"));
////            usageInfo.put("usage_info", usageResult);
////            usageList.add(usageInfo);
//        }
//
//        List<String> selList = List.of(sel.split(","));
//        String selFirst = selList.getFirst();
//        int indexOfAs = selFirst.indexOf(" AS ");
//        if(indexOfAs == -1){
//            indexOfAs = selFirst.indexOf(" as ");
//            if(indexOfAs == -1){
//                logger.severe("AS not found in sel");
//                return Map.of("error", "AS not found in sel");
//            }
//        }
//        String selAs = selFirst.substring(indexOfAs + 4);
//
//        sqlUsageList.append(" ORDER BY ").append(selAs).append(" DESC NULLS LAST ").append(" LIMIT ").append(topStr);
//
//        List<Map<String, Object>> resp;
//        try {
//            resp = oqgHelper.OqgR2x(sqlUsageList.toString(), "HARV", true);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return Map.of("error", e.getMessage());
//        }
//        if(resp.isEmpty()){
//            return Map.of("error", "No data found");
//        }
//        List<Map<String, Object>> usageList = new ArrayList<>(resp);
//
//        return Map.of("scope_usage_list", usageList);
//    }
//
//    public Map<String, Object> getScopeFleetHealthList(Map<String, Object> request, GetFleetHealth getFleetHealth) {
//        logger.info("getScopeFleetHealthList");
//
//        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
//        if (scope == null) {
//            return Map.of("error", "scope is required");
//        }
//
//        List<Map<String, Object>> scopeInfoList = (List<Map<String, Object>>) request.get("scope_info_list");
//        if (scopeInfoList == null) {
//            logger.severe("scope_info_list is required");
//            return Map.of("error", "scope_info_list is required");
//        }
//        String childrenScopeTypeStr = (String) request.get("children_scope_type");
//        if (childrenScopeTypeStr == null) {
//            logger.severe("children_scope_type is required");
//            return Map.of("error", "children_scope_type is required");
//        }
//        PagScopeTypeEnum childrenScopeType;
//        try {
//            childrenScopeType = PagScopeTypeEnum.valueOf(childrenScopeTypeStr);
//        } catch (IllegalArgumentException e) {
//            logger.severe("Invalid list_scope_type");
//            return Map.of("error", "Invalid list_scope_type");
//        }
//
//        List<Map<String, Object>> fhList = new ArrayList<>();
//        for (Map<String, Object> scopeInfo : scopeInfoList) {
//
//            Map<String, Object> itemScope = new HashMap<>();
//            String itemId = (String) scopeInfo.get("id");
//            String itemName = (String) scopeInfo.get("name");
//
//            itemScope.put("project_name", scope.get("project_name"));
//            itemScope.put("project_id", scope.get("project_id"));
//
//            switch (childrenScopeType) {
//                case location -> {
//                    itemScope.put("location_id", itemId);
//                    itemScope.put("location_name", itemName);
//                }
//                case locationGroup -> {
//                    itemScope.put("location_group_id", itemId);
//                    itemScope.put("location_group_name", itemName);
//                }
//                case building -> {
//                    itemScope.put("building_id", itemId);
//                    itemScope.put("building_name", itemName);
//                }
//                case site -> {
//                    itemScope.put("site_id", itemId);
//                    itemScope.put("site_name", itemName);
//                }
//                case siteGroup -> {
//                    itemScope.put("site_group_id", itemId);
//                    itemScope.put("site_group_name", itemName);
//                }
//                default -> {
//                    logger.severe("Invalid list_scope_type");
//                    return Map.of("error", "Invalid list_scope_type");
//                }
//            }
//
//            Map<String, Object> fhRequest = new HashMap<>();
//            fhRequest.put("scope", itemScope);
//            Map<String, Object> fhResult = getFleetHealth.run(fhRequest);
//            if (fhResult.get("error") != null) {
//                logger.severe((String) fhResult.get("error"));
//                continue;
//            }
////            scopeInfo.put("fleet_health", fhResult.get("fleet_health"));
//            Map<String, Object> fhInfo = new HashMap<>();
//            fhInfo.put("id", scopeInfo.get("id"));
//            fhInfo.put("name", scopeInfo.get("name"));
//            fhInfo.put("fleet_health", fhResult.get("fleet_health"));
//            fhList.add(fhInfo);
//        }
//
//        return Map.of("scope_fleet_health_list", fhList);
//    }
//
//    public Map<String, Object> getListInfo(Map<String, Object> request) {
//        logger.info("getListInfo");
//
//        String listInfoTableName = (String) request.get("list_info_table_name");
//        if (listInfoTableName == null || listInfoTableName.isEmpty()) {
//            return Map.of("error", "list_info_table_name is required");
//        }
//        String listTypeName = (String) request.get("list_type_name");
//        if (listTypeName == null || listTypeName.isEmpty()) {
//            return Map.of("error", "list_type_name is required");
//        }
//
//        //get list config
//        List<Map<String, Object>> listInfoResp;
//        try {
//            String sqlListConfig =
////                    "SELECT root_table_name, filter_key_equal, filter_key_like, join_key, list_config "
//                    "SELECT * FROM " + listInfoTableName + " WHERE list_type_name = '" + listTypeName + "'";
//            listInfoResp = oqgHelper.OqgR2x(sqlListConfig, "OPS", true);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return Map.of("error", e.getMessage());
//        }
//        if(listInfoResp.isEmpty()){
//            logger.info("list info not found");
//            return Map.of("error", "list info not found");
//        }
//
//        Map<String, Object> result = new HashMap<>();
//
////        String filterKeyEqual = (String) listInfoResp.getFirst().get("filter_key_equal");
////        if (filterKeyEqual != null && !filterKeyEqual.isEmpty()) {
////            List<String> filterKeyEqualList = List.of(filterKeyEqual.split(";"));
////            result.put("filter_key_equal_list", filterKeyEqualList);
////        }
//
////        String filterKeyLike = (String) listInfoResp.getFirst().get("filter_key_like");
////        if (filterKeyLike != null && !filterKeyLike.isEmpty()) {
////            List<String> filterKeyLikeList = List.of(filterKeyLike.split(";"));
////            result.put("filter_key_like_list", filterKeyLikeList);
////        }
//
//        String joinKey = (String) listInfoResp.getFirst().get("join_key");
//        if (joinKey != null && !joinKey.isEmpty()) {
////            List<String> joinKeyList = List.of(joinKey.split(";"));
////            result.put("join_key_list", joinKeyList);
//            //parse json string into joinList
//            //e.g. [{"join_table":"sunseap.location_sunseap","join_sel":"sunseap.location_sunseap.label as location_label","on":"sunseap.gateway_sunseap.location_id=sunseap.location_sunseap.id"},{"join_table":"sunseap.site_sunseap","join_sel":"sunseap.site_sunseap.label as site_label,sunseap.site_sunseap.id as site_id","on":"sunseap.location_sunseap.site_id=sunseap.site_sunseap.id"}]
//            ObjectMapper objectMapper = new ObjectMapper();
//            try {
//                List<Map<String, Object>> joinList = objectMapper.readValue(joinKey, List.class);
//                result.put("join_key_list", joinList);
//            } catch (Exception e) {
//                logger.severe(e.getMessage());
//                return Map.of("error", e.getMessage());
//            }
//        }
//
//        String enableJoinKey = (String) listInfoResp.getFirst().get("enable_join_key");
//        if (enableJoinKey != null && !enableJoinKey.isEmpty()) {
//            result.put("enable_join_key", enableJoinKey);
//        }
//
//        String enableGroupBy = (String) listInfoResp.getFirst().get("enable_group_by");
//        if (enableGroupBy != null && !enableGroupBy.isEmpty()) {
//            result.put("enable_group_by", enableGroupBy);
//        }
//
//        //from json string to map
//        ObjectMapper objectMapper = new ObjectMapper();
//        try {
//            String jsonString = (String) listInfoResp.getFirst().get("list_config");
//            List<Map<String, Object>> listConfig = objectMapper.readValue(jsonString, List.class);
//            result.put("list_config", listConfig);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return Map.of("error", e.getMessage());
//        }
//        result.put("root_table_name", listInfoResp.getFirst().get("root_table_name"));
//
//        return Map.of("list_info", result);
//    }
//
//    @Deprecated
//    public Map<String, Object> getListInfoList(Map<String, Object> request, GetProjectItemListTypeName getItemListTypeNameCallback) {
//        logger.info("getListInfoList()");
//
//        String itemKindStr = (String) request.get("item_kind");
//
//        String listInfoTableName = (String) request.get("list_info_table_name");
//        if (listInfoTableName == null || listInfoTableName.isEmpty()) {
//            return Map.of("error", "list_info_table_name is required");
//        }
//
//        String itemType = (String) request.get("item_type");
//        String itemTypeListStr = (String) request.get("item_type_list_str");
//
//        List<String> itemTypeList = new ArrayList<>();
//        if(itemType != null && !itemType.isEmpty()){
//            itemTypeList.add(itemType);
//        }else if (itemTypeListStr != null && !itemTypeListStr.isEmpty()) {
//            itemTypeList = Arrays.asList(itemTypeListStr.split(","));
//        }
//
//        List<Map<String, Object>> listInfoList = new ArrayList<>();
//
//        List<Map<String, Object>> listInfoTypeNameList = new ArrayList<>();
//        if (itemTypeList.isEmpty() || "NOT_SET".equals(itemTypeList.getFirst())) {
//            Map<String, Object> itemListTypeNameResult = getItemListTypeNameCallback.run(itemKindStr, "");
//            if (itemListTypeNameResult.get("error") != null) {
//                return Map.of("error", itemListTypeNameResult.get("error"));
//            }
//            String listTypeName = (String) itemListTypeNameResult.get("list_type_name");
//            if (listTypeName == null || listTypeName.isEmpty()) {
//                return Map.of("error", "list_type_name is required");
//            }
//            listInfoTypeNameList.add(Map.of("list_type_name", listTypeName, "item_type", ""));
//        } else {
//            for (String itemTypeStr : itemTypeList) {
//                Map<String, Object> itemListTypeNameResult = getItemListTypeNameCallback.run(itemKindStr, itemTypeStr);
//                if (itemListTypeNameResult.get("error") != null) {
//                    return Map.of("error", itemListTypeNameResult.get("error"));
//                }
//                String listTypeName = (String) itemListTypeNameResult.get("list_type_name");
//                if (listTypeName == null || listTypeName.isEmpty()) {
//                    return Map.of("error", "list_type_name is required");
//                }
//                listInfoTypeNameList.add(Map.of("list_type_name", listTypeName, "item_type", itemTypeStr));
//            }
//        }
//        for (Map<String, Object> listTypeName : listInfoTypeNameList) {
//            String listTypeNameStr = (String) listTypeName.get("list_type_name");
//            String itemTypeStr = (String) listTypeName.get("item_type");
//
//            request.put("list_type_name", listTypeNameStr);
//            request.put("item_type", itemTypeStr);
//            Map<String, Object> listInfo = getListInfo(request);
//            if (listInfo.get("error") != null) {
//                continue;
////                return Map.of("error", listInfo.get("error"));
//            }
//            Map<String, Object> listInfoMap = new HashMap<>((Map<String, Object>) listInfo.get("list_info"));
//            listInfoMap.put("item_type", itemTypeStr);
//            listInfoList.add(listInfoMap);
//        }
//        return Map.of("list_info_list", listInfoList);
//    }
//
//    /**
//     * get list info list with provided item kind / item type
//     * @param request
//     * @param getItemListTypeNameCallback
//     * @return
//     */
//    public Map<String, Object> getListInfoList2(Map<String, Object> request, GetProjectItemListTypeName getItemListTypeNameCallback) {
//        logger.info("getListInfoList2()");
//
//        String itemKindStr = (String) request.get("item_kind");
//
//        String listInfoTableName = (String) request.get("list_info_table_name");
//        if (listInfoTableName == null || listInfoTableName.isEmpty()) {
//            return Map.of("error", "list_info_table_name is required");
//        }
//
//        String itemType = (String) request.get("item_type");
//        String itemTypeListStr = (String) request.get("item_type_list_str");
//
//        List<String> itemTypeList = new ArrayList<>();
//        if(itemType != null && !itemType.isEmpty()){
//            itemTypeList.add(itemType);
//        }else if (itemTypeListStr != null && !itemTypeListStr.isEmpty()) {
//            itemTypeList = Arrays.asList(itemTypeListStr.split(","));
//        }
//
////        List<Map<String, Object>> listInfoList = new ArrayList<>();
//
//        List<Map<String, Object>> listInfoTypeInfoList = new ArrayList<>();
//        if (itemTypeList.isEmpty() || "NOT_SET".equals(itemTypeList.getFirst())) {
//            Map<String, Object> itemListTypeInfoListResult = getItemListTypeNameCallback.run(itemKindStr, "");
//            if (itemListTypeInfoListResult.get("error") != null) {
//                return Map.of("error", itemListTypeInfoListResult.get("error"));
//            }
//            List<Map<String, Object>> listTypeInfoList = (List<Map<String, Object>>) itemListTypeInfoListResult.get("list_type_info_list");
//            if (listTypeInfoList == null || listTypeInfoList.isEmpty()) {
//                return Map.of("error", "list_type_info_list is missing");
//            }
//            listInfoTypeInfoList.addAll(listTypeInfoList);
//        } else {
//            for (String itemTypeStr : itemTypeList) {
//                Map<String, Object> itemListTypeInfoListResult = getItemListTypeNameCallback.run(itemKindStr, itemTypeStr);
//                if (itemListTypeInfoListResult.get("error") != null) {
//                    return Map.of("error", itemListTypeInfoListResult.get("error"));
//                }
//                List<Map<String, Object>> listTypeInfoList = (List<Map<String, Object>>) itemListTypeInfoListResult.get("list_type_info_list");
//                if (listTypeInfoList == null || listTypeInfoList.isEmpty()) {
//                    return Map.of("error", "list_type_info_list is missing");
//                }
//
//                listInfoTypeInfoList.addAll(listTypeInfoList);
//            }
//        }
//        List<Map<String, Object>> listInfoList = new ArrayList<>();
//        for (Map<String, Object> listTypeInfo : listInfoTypeInfoList) {
//            String ltn = (String) listTypeInfo.get("list_type_name");
//            String it = (String) listTypeInfo.get("item_type");
//
//            request.put("list_type_name", ltn);
//            request.put("item_type", it);
//            Map<String, Object> listInfo = getListInfo(request);
//            if (listInfo.get("error") != null) {
//                continue;
//            }
//            Map<String, Object> listInfoMap = new HashMap<>((Map<String, Object>) listInfo.get("list_info"));
//            listInfoMap.put("item_type", it);
//            listInfoList.add(listInfoMap);
//        }
//
//        return Map.of("list_info_list", listInfoList);
//    }
//
//    public Map<String, Object> getServiceInfo(Map<String, Object> request) {
//        logger.info("getServiceInfo()");
//
//        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
//        if (scope == null || scope.isEmpty()) {
//            return Map.of("error", StdErrorDto.builder().code(ApiCode.REQUEST_MISSING_PARAMETER).message("Missing required parameter: scope").build());
//        }
//        String projectId = (String) scope.get("project_id");
//        String projectName = (String) scope.get("project_name");
////        if (projectId == null || projectId.isEmpty()) {
////            return Map.of("error", StdErrorDto.builder().code(ApiCode.REQUEST_MISSING_PARAMETER).message("Missing required parameter: project_id").build());
////        }
//        if (projectName == null || projectName.isEmpty()) {
//            return Map.of("error", StdErrorDto.builder().code(ApiCode.REQUEST_MISSING_PARAMETER).message("Missing required parameter: project_name").build());
//        }
//
//        String itemId = (String) request.get("item_id");
//        if(itemId == null || itemId.isEmpty()){
//            return Map.of("error", StdErrorDto.builder()
//                    .code(ApiCode.REQUEST_MISSING_PARAMETER)
//                    .message("item_id is required")
//                    .build());
//        }
//
//        String itemType = (String) request.get("item_type");
//        if(itemType == null || itemType.isEmpty()){
//            return Map.of("error", StdErrorDto.builder()
//                    .code(ApiCode.REQUEST_MISSING_PARAMETER)
//                    .message("item_type is required")
//                    .build());
//        }
//
//        String serviceType = (String) request.get("service_type");
//        if(serviceType == null || serviceType.isEmpty()){
//            return Map.of("error", StdErrorDto.builder()
//                    .code(ApiCode.REQUEST_MISSING_PARAMETER)
//                    .message("service_type is required")
//                    .build());
//        }
//
//        String serviceTableName = ProjectOreHelper.getProjectItemTableName(projectName, "service");
//        String itemTableName = ProjectOreHelper.getProjectItemTableName(projectName, itemType);
//
//        String serviceAddressColumnName;
//        if ("dev".equals(serviceAddressType)) {
//            serviceAddressColumnName = "address_dev";
//        } else if ("deploy-test".equals(serviceAddressType)) {
//            serviceAddressColumnName = "address_deploy_test";
//        } else {
//            serviceAddressColumnName = "address_deploy";
//        }
//
//        String sql = "SELECT " + serviceAddressColumnName + " FROM " + serviceTableName + " s "
//                + " LEFT JOIN " + itemTableName + " i ON i." + serviceType + "_service_id = s.id";
//
//        List<Map<String, Object>> resp;
//        try {
//            resp = oqgHelper.OqgR2x(sql, "OPS", true);
//        } catch (Exception e) {
//            return Map.of("error", StdErrorDto.builder().code(ApiCode.RESULT_DATABASE_ERROR).message(e.getMessage()).build());
//        }
//        if(resp.isEmpty()) {
//            return Map.of("error", StdErrorDto.builder().code(ApiCode.RESULT_NOT_FOUND).message("Service not found").build());
//        }
//        Map<String, Object> serviceInfo = resp.getFirst();
//
//        String serviceAddress = (String) serviceInfo.get(serviceAddressColumnName);
//        if(serviceAddress == null || serviceAddress.isEmpty()){
//            return Map.of("error", StdErrorDto.builder().code(ApiCode.RESULT_NOT_FOUND).message("Service address not found").build());
//        }
//        serviceInfo.put("service_address", serviceAddress);
//        serviceInfo.remove(serviceAddressColumnName);
//        serviceInfo.put("service_address_type", serviceAddressType);
//
//        return Map.of("service_info", serviceInfo);
//    }
//
//}
