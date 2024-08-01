package org.pabuff.evs2helper.meter_usage;

import org.pabuff.dto.ItemIdTypeEnum;
import org.pabuff.dto.ItemTypeEnum;
import org.pabuff.evs2helper.locale.LocalHelper;
import org.pabuff.evs2helper.scope.ScopeHelper;
import org.pabuff.oqghelper.OqgHelper;
import org.pabuff.oqghelper.QueryHelper;
import org.pabuff.utils.DateTimeUtil;
import org.pabuff.utils.MathUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;

@Service
public class MeterUsageProcessor {
    Logger logger = Logger.getLogger(MeterUsageProcessor.class.getName());

    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private QueryHelper queryHelper;
    @Autowired
    private ScopeHelper scopeHelper;
    @Autowired
    private LocalHelper localHelper;

    //for list of single meter usage history
    public Map<String, Object> getMeterListUsageSummary(Map<String, String> request, List<Map<String, Object>> meterList) {
        logger.info("process getMeterListUsageSummary");

        int testCount = Integer.parseInt(request.getOrDefault("test_count", "0"));

        Long mbrRangeHours = Long.parseLong(request.getOrDefault("mbr_range_hours", "3"));

        String projectScope = request.get("project_scope");
        String siteScope = request.get("site_scope");
        String meterSelectSql = request.get("id_select_query") == null ? "" : request.get("id_select_query");

        String startDatetimeStr = request.get("from_timestamp")==null ? request.get("start_datetime") : request.get("from_timestamp");
        String endDatetimeStr = request.get("to_timestamp")==null ? request.get("end_datetime") : request.get("to_timestamp");

        String itemIdTypeStr = request.get("item_id_type")==null?"":request.get("item_id_type");

        String isMonthlyStr = request.get("is_monthly");
        boolean isMonthly = false;
        if (isMonthlyStr != null && !isMonthlyStr.isEmpty()) {
            isMonthly = Boolean.parseBoolean(isMonthlyStr);
        }

        String meterTypeStr = request.get("item_type");
        ItemTypeEnum meterTypeEnum = null;
        if(meterTypeStr != null) {
            meterTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig2(projectScope, itemIdTypeStr);
        if(meterTypeEnum==null){
            meterTypeEnum = ItemTypeEnum.valueOf((String)itemConfig.get("itemTypeEnum"));
        }
        String itemReadingTableName = (String) itemConfig.get("itemReadingTableName");
        String itemTableName = (String) itemConfig.get("itemTableName");
        String itemReadingIndexColName = (String) itemConfig.get("itemReadingIndexColName");
        String itemIdColName = (String) itemConfig.get("itemIdColName");
        String itemReadingIdColName = (String) itemConfig.get("itemReadingIdColName");
        String itemSnColName = (String) itemConfig.get("itemSnColName");
        String itemNameColName = (String) itemConfig.get("itemNameColName");
        String itemAltName = (String) itemConfig.get("itemAltNameColName");
        String panelTagColName = (String) itemConfig.get("panelTagColName");
        String itemIdColSel = (String) itemConfig.get("itemIdColSel");
        String itemLocColSel = (String) itemConfig.get("itemLocColSel") == null ? "" : (String) itemConfig.get("itemLocColSel");
        String timeKey =(String) itemConfig.get("timeKey");
        String valKey = (String) itemConfig.get("valKey");

        String sortBy = request.get("sort_by");
        String sortOrder = request.get("sort_order");

        Map<String, Object> result = new HashMap<>();

        int limit = Integer.parseInt(request.getOrDefault("max_rows_per_page", "20"));
        int page = Integer.parseInt(request.getOrDefault("current_page", "1"));
        int offset = (page - 1) * limit;
        String getCount = request.get("get_count");
        boolean getCountBool = !meterSelectSql.isEmpty();
        if(getCount != null && !getCount.isEmpty()){
            getCountBool = Boolean.parseBoolean(getCount);
        }
        //get count if it's the first page
        if (page == 1 && getCountBool) {
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

        String meterSelectSql2 = meterSelectSql;

        if(meterTypeEnum == ItemTypeEnum.METER_3P){
            itemLocColSel = "";
        }
        String replacementStr = "SELECT " + itemIdColSel + ", meter_type, commissioned_timestamp, last_reading_timestamp";
        if(itemLocColSel != null && !itemLocColSel.isEmpty()){
            replacementStr += ", " + itemLocColSel;
        }
        meterSelectSql2 = meterSelectSql2.replace("SELECT " + itemIdColName, replacementStr);

//        String additionalCols = "";
//        if(!meterSelectSql2.contains("commissioned_timestamp")) {
//            additionalCols = ", commissioned_timestamp";
//            //            meterSelectSql2 = meterSelectSql2.replace("SELECT " + itemIdColSel, "SELECT " + itemIdColSel + ", commissioned_timestamp");
//        }
//        if(!meterSelectSql2.contains("meter_type")){
//            additionalCols += ", meter_type";
//        }
//        meterSelectSql2 = meterSelectSql2.replace("SELECT " + itemIdColSel, "SELECT " + itemIdColSel + additionalCols);

//        meterSelectSql2 += ", " + itemLocColSel;
        meterSelectSql2 += " ORDER BY " + itemIdColName + " LIMIT " + limit + " OFFSET " + offset;

        List<Map<String, Object>> selectedMeterList;
        if(meterList != null) {
            selectedMeterList = meterList;
        }else {
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
            selectedMeterList = resp;
        }

        List<Map<String, Object>> usageSummaryList = new ArrayList<>();
        int processingCount = 0;
        int totalCount = selectedMeterList.size();
        for (Map<String, Object> meterMap : selectedMeterList) {
            String meterId = (String) meterMap.get(itemIdColName);
            String readingMeterId = (String) meterMap.get(itemReadingIdColName);
            String meterSn = meterMap.get(itemSnColName) == null ? "" : (String) meterMap.get(itemSnColName);
            String meterName = meterMap.get(itemNameColName) == null ? "" : (String) meterMap.get(itemNameColName);
            String meterAltName = meterMap.get(itemAltName) == null ? "" : (String) meterMap.get(itemAltName);
            String meterLcStatus = meterMap.get("lc_status") == null ? "" : (String) meterMap.get("lc_status");
            String commissionedTimestampStr = meterMap.get("commissioned_timestamp") == null ? "" : (String) meterMap.get("commissioned_timestamp");
            String meterType = meterMap.get("meter_type") == null ? "" : (String) meterMap.get("meter_type");
            LocalDateTime commissionedDatetime = DateTimeUtil.getLocalDateTime(commissionedTimestampStr);
            String lastReadingTimestampStr = meterMap.get("last_reading_timestamp") == null ? "" : (String) meterMap.get("last_reading_timestamp");

            processingCount++;
//            Integer commissionedYear = null;
//            Integer commissionedMonth = null;
//            if(commissionedDatetime != null){
//                commissionedYear = commissionedDatetime.getYear();
//                commissionedMonth = commissionedDatetime.getMonthValue();
//            }

            if (meterId == null || meterId.isEmpty()) {
                logger.info("meterId is null or empty");
                continue;
            }

//            if(!meterId.equals("E019")){
//                continue;
//            }

            LinkedHashMap<String, Object> usageSummary = new LinkedHashMap<>();

            String[] idColList = itemIdColSel.split(",");
            String[] locColList = itemLocColSel != null ? itemLocColSel.split(",") : null;
            for(String idCol : idColList){
                usageSummary.put(idCol, meterMap.get(idCol));
            }
            if(locColList != null){
                for(String locCol : locColList){
                    usageSummary.put(locCol, meterMap.get(locCol));
                }
            }
            usageSummary.put("site_tag", meterMap.get("site_tag"));
            if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
                usageSummary.put("meter_type", meterMap.get("meter_type"));
            }
            // fields are in order for LinkedHashMap
            if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
                usageSummary.put("lc_status", meterLcStatus.isEmpty()?"-":meterLcStatus);
            }
            usageSummary.put("commissioned_timestamp", commissionedTimestampStr);

            String mbrAdditionalConstraint = "";
            if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
                mbrAdditionalConstraint = " (duplicated != true OR duplicated is null) AND (interpolated != true OR interpolated is null) ";
            }

            if (isMonthly) {
                Map<String, Object> resultMonthly =
                        findMonthlyReading(
                                commissionedDatetime,
//                        startDatetimeStr,
                                mbrRangeHours,
                                endDatetimeStr,
                                meterId,
                                itemReadingTableName,
                                itemReadingIndexColName,
                                itemReadingIdColName,//itemIdColName,
                                timeKey, valKey,
                                mbrAdditionalConstraint);

                if (resultMonthly.containsKey("error")) {
                    logger.info("error: " + resultMonthly.get("error"));
                    return Collections.singletonMap("error", "error: " + resultMonthly.get("error"));
                }
                if (resultMonthly.containsKey("info")) {
                    logger.info("info: " + resultMonthly.get("info"));
//                    return Collections.singletonMap("info", "info: " + resultMonthly.get("info"));
                    continue;
                }

//                usageSummary.put(itemSnColName, meterSn);
//                usageSummary.put(itemNameColName, meterName);

//                if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
//                    usageSummary.put("alt_name", meterAltName);
//                }else if(meterTypeEnum == ItemTypeEnum.METER_3P){
//                    usageSummary.put("panel_tag", meterMap.get("panel_tag"));
//                }
//                usageSummary.put("alt_name", meterAltName);

                String firstReadingVal = (String) resultMonthly.get("first_reading_val");
                String lastReadingVal = (String) resultMonthly.get("last_reading_val");
                String firstReadingTime = ((String) resultMonthly.get("first_reading_time")).isEmpty() ? "-" : (String) resultMonthly.get("first_reading_time");
                String lastReadingTime = ((String) resultMonthly.get("last_reading_time")).isEmpty() ? "-" : (String) resultMonthly.get("last_reading_time");
                Double firstReadingValDouble = firstReadingVal.isEmpty()? null : Double.parseDouble(firstReadingVal);
                Double lastReadingValDouble = lastReadingVal.isEmpty()? null: Double.parseDouble(lastReadingVal);
                //round to 2 decimals
                Double firstReadingValDouble2 = firstReadingValDouble==null? null : MathUtil.setDecimalPlaces(firstReadingValDouble, 2, RoundingMode.HALF_UP);
                Double lastReadingValDouble2 = lastReadingValDouble==null? null: MathUtil.setDecimalPlaces(lastReadingValDouble, 2, RoundingMode.HALF_UP);
                Double usageDouble = firstReadingValDouble2==null || lastReadingValDouble2==null ? null : lastReadingValDouble2 - firstReadingValDouble2;
//                Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                firstReadingVal = firstReadingValDouble2==null? "-" : String.format("%.2f", firstReadingValDouble2);
                lastReadingVal = lastReadingValDouble2==null? "-" : String.format("%.2f", lastReadingValDouble2);
                String usage = usageDouble==null? "-" : String.format("%.2f", usageDouble);

                usageSummary.put("first_reading_time", firstReadingTime);
                usageSummary.put("last_reading_time", lastReadingTime);
                usageSummary.put("first_reading_val", firstReadingVal);
                usageSummary.put("last_reading_val", lastReadingVal);
                usageSummary.put("usage", usage);
//                usageSummary.put("first_reading_ref", resultMonthly.get("first_reading_ref"));
//                usageSummary.put("last_reading_ref", resultMonthly.get("last_reading_ref"));
                boolean useCommissionDatetime = resultMonthly.get("use_commissioned_datetime") != null && (boolean) resultMonthly.get("use_commissioned_datetime");
                usageSummary.put("use_commissioned_datetime", useCommissionDatetime);
                usageSummary.put("last_reading_timestamp", lastReadingTimestampStr);

                usageSummaryList.add(usageSummary);

                logger.info(processingCount + "/" + totalCount + " meter: " + meterSn + " usage: " + usage);
                if(testCount > 0 && processingCount >= testCount){
                    break;
                }
                continue;
            }

            // get first and last dt between start and end datetime,
            // and get val at first and last dt
            String sql = "SELECT DISTINCT " +
                    " FIRST_VALUE(" + valKey + ") OVER w AS first_reading_val," +
                    " LAST_VALUE(" + valKey + ") OVER w AS last_reading_val," +
                    " FIRST_VALUE(" + timeKey + ") OVER w AS first_reading_time," +
                    " LAST_VALUE(" + timeKey + ") OVER w AS last_reading_time " +
                    " FROM " + itemReadingTableName + " WHERE " +
//                    itemIdColName + " = '" + meterId + "' AND " +
                    itemReadingIdColName + " = '" + readingMeterId + "' AND " +
                    timeKey + " BETWEEN '" + startDatetimeStr + "' AND '" + endDatetimeStr + "'" +
                    " WINDOW w AS ( ORDER BY " + timeKey + " RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";

            List<Map<String, Object>> resp2;
            try {
                resp2 = oqgHelper.OqgR2(sql, true);
            } catch (Exception e) {
                logger.info("oqgHelper error: " + e.getMessage() + " for meter: " + meterSn);
                return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
            }
            if (resp2 == null) {
                logger.info("oqgHelper error: resp is null" + " for meter: " + meterSn);
                return Collections.singletonMap("error", "oqgHelper error: resp is null");
            }
            if (resp2.isEmpty()) {
                logger.info("no reading found for meter: " + meterSn);
//                return Collections.singletonMap("info", "no reading found for meter: " + meterSn);
            }
            String firstReadingVal = "-";
            String lastReadingVal = "-";
            String firstReadingTime = "-";
            String lastReadingTime = "-";
            String usage = "-";
            if(resp2.isEmpty()){
            }else {
                firstReadingVal = (String) resp2.getFirst().get("first_reading_val");
                lastReadingVal = (String) resp2.getFirst().get("last_reading_val");
                firstReadingTime = (String) resp2.getFirst().get("first_reading_time");
                lastReadingTime = (String) resp2.getFirst().get("last_reading_time");
                Double firstReadingValDouble = firstReadingVal.isEmpty()? null : Double.parseDouble(firstReadingVal);
                Double lastReadingValDouble = lastReadingVal.isEmpty()? null: Double.parseDouble(lastReadingVal);
                //round to 2 decimals
                Double firstReadingValDouble2 = firstReadingValDouble==null? null : MathUtil.setDecimalPlaces(firstReadingValDouble, 2, RoundingMode.HALF_UP);
                Double lastReadingValDouble2 = lastReadingValDouble==null? null: MathUtil.setDecimalPlaces(lastReadingValDouble, 2, RoundingMode.HALF_UP);
                Double usageDouble = firstReadingValDouble2==null || lastReadingValDouble2==null ? null : lastReadingValDouble2 - firstReadingValDouble2;
//                Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                firstReadingVal = firstReadingValDouble2==null? "-" : String.format("%.2f", firstReadingValDouble2);
                lastReadingVal = lastReadingValDouble2==null? "-" : String.format("%.2f", lastReadingValDouble2);
                usage = usageDouble==null? "-" : String.format("%.2f", usageDouble);
            }
//            LinkedHashMap<String, Object> usageSummary = new LinkedHashMap<>();
//
//            String[] idColList = itemIdColSel.split(",");
//            String[] locColList = itemLocColSel.split(",");
//            for(String idCol : idColList){
//                usageSummary.put(idCol, meterMap.get(idCol));
//            }
//            for(String locCol : locColList){
//                usageSummary.put(locCol, meterMap.get(locCol));
//            }
//
//            if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
//                usageSummary.put("lc_status", meterLcStatus);
//            }

            usageSummary.put("first_reading_time", firstReadingTime);
            usageSummary.put("last_reading_time", lastReadingTime);
            usageSummary.put("first_reading_val", firstReadingVal);
            usageSummary.put("last_reading_val", lastReadingVal);
            usageSummary.put("usage", usage);

            usageSummaryList.add(usageSummary);

            logger.info(processingCount + "/" + totalCount + " meter: " + meterSn + " usage: " + usage);
            if(testCount > 0 && processingCount >= testCount){
                break;
            }
        }
        result.put("meter_list_usage_summary", usageSummaryList);
        return result;
    }

    // for multiple meters consolidated usage history
    public Map<String, Object> getMeterConsolidatedUsageHistory(
            Map<String, String> request) {
        logger.info("process getMeterConsolidatedUsageHistory");

        Long mbrRangeHours = Long.parseLong(request.getOrDefault("mbr_range_hours", "3"));

        String projectScope = request.get("project_scope");
        String siteScope = request.get("site_scope");
//        String meterSelectSql = request.get("id_select_query");
//        String startDatetimeStr = request.get("start_datetime");
        String endDatetimeStr = request.get("end_datetime");

        String targetInterval = request.get("target_interval");
        String numberOfIntervalsStr = request.get("num_of_intervals");
        int numberOfIntervals = Integer.parseInt(numberOfIntervalsStr);

        String itemTypeStr = request.get("item_type");
        if(itemTypeStr == null || itemTypeStr.isEmpty()){
            return Collections.singletonMap("error", "Invalid request");
        }
        ItemTypeEnum itemTypeEnum = ItemTypeEnum.valueOf(itemTypeStr.toUpperCase());

        String itemIdTypeStr = request.get("item_id_type");
        if(itemIdTypeStr == null || itemIdTypeStr.isEmpty()){
            return Collections.singletonMap("error", "Invalid request");
        }
        ItemIdTypeEnum itemIdTypeEnum = ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());

        String meterGroupName = request.get("group_name");
        Map<String, Object> percentMap = new HashMap<>();
        if(meterGroupName!=null && !meterGroupName.isEmpty()){
            Map<String, Object> result = queryHelper.getTableField(
                    "meter_group", "id", "name", meterGroupName);
            String meterGroupIndexStr = (String) result.get("id");
            if(meterGroupIndexStr == null || meterGroupIndexStr.isEmpty()){
                return Collections.singletonMap("error", "Invalid request");
            }
            //get meter percentage from meter group
            String sql = "SELECT meter_id, percentage FROM meter_group_meter_iwow WHERE meter_group_id = '" + meterGroupIndexStr + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2(sql, true);
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
            List<Map<String, Object>> meterList = resp;
            for(Map<String, Object> meterMap : meterList){
                String meterId = (String) meterMap.get("meter_id");
                String percentage = (String) meterMap.get("percentage");
                percentMap.put(meterId, percentage);
            }
        }

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig2(projectScope, itemIdTypeStr);
        String itemReadingTableName = (String) itemConfig.get("itemReadingTableName");
        String itemReadingIndexColName = (String) itemConfig.get("itemReadingIndexColName");
        String itemReadingIdColName = (String) itemConfig.get("itemReadingIdColName");
        String itemTableName = (String) itemConfig.get("itemTableName");
        String itemIdColName = (String) itemConfig.get("itemIdColName");
        String itemSnColName = (String) itemConfig.get("itemSnColName");
        String itemNameColName = (String) itemConfig.get("itemNameColName");
        String timeKey = (String) itemConfig.get("timeKey");
        String valKey = (String) itemConfig.get("valKey");

        // get meter list
        String meterIdListStr = request.get("item_id_list");
        String[] meterIdList = meterIdListStr.split(",");

        List<String> meterIdList2 = new ArrayList<>();
        for(String meterId : meterIdList){
            if(meterId == null || meterId.isEmpty()){
                continue;
            }
            meterIdList2.add(meterId);
        }
        if(itemTypeEnum == ItemTypeEnum.METER_3P){
            if (itemIdTypeEnum != ItemIdTypeEnum.ID) {
                meterIdList2.clear();
                for (String meterId : meterIdList) {
                    String meterIdColName = "";
                    if (itemIdTypeEnum == ItemIdTypeEnum.SN) {
                        meterIdColName = "meter_sn";
                    } else if (itemIdTypeEnum == ItemIdTypeEnum.PANEL_TAG) {
                        meterIdColName = "panel_tag";
                    }
                    Map<String, Object> result = queryHelper.getTableField(
                            "meter_3p", "meter_id", meterIdColName, meterId);
                    String meter_id = (String) result.get("meter_id");
                    if (meter_id != null && !meter_id.isEmpty()) {
                        meterIdList2.add(meter_id);
                    }
                }
            }
        }else if(itemTypeEnum == ItemTypeEnum.METER_IWOW){
            if (itemIdTypeEnum != ItemIdTypeEnum.NAME && itemIdTypeEnum != ItemIdTypeEnum.SN ) {
                meterIdList2.clear();
                for (String meterId : meterIdList) {
                    String meterIdColName = "";
                    if (itemIdTypeEnum == ItemIdTypeEnum.ALT_NAME) {
                        meterIdColName = "alt_name";
                    }
                    Map<String, Object> result = queryHelper.getTableField(
                            "meter_iwow", "item_sn", meterIdColName, meterId);
                    String meterIdStr = (String) result.get("item_sn");
                    if (meterIdStr != null && !meterIdStr.isEmpty()) {
                        meterIdList2.add(meterIdStr);
                    }
                }
            }
        }

        List<Map<String, Object>> meterListConsumptionHistory = new ArrayList<>();
        for (String meterId : meterIdList2) {
            if (meterId == null || meterId.isEmpty()) {
                logger.info("meterId is null or empty");
                continue;
            }

            String meterIndex = meterId;
            if(itemIdTypeEnum != ItemIdTypeEnum.ID){
                Map<String, Object> result = queryHelper.getTableField(
                        itemTableName, "id", itemIdColName, meterId);
                String meterIdStr = (String) result.get("id");
                if (meterIdStr == null || meterIdStr.isEmpty()) {
                    logger.info("meter_id is null or empty");
                    continue;
                }
                meterIndex = meterIdStr;
            }
            Double percentage = percentMap.get(meterIndex) == null ? 100.0 : Double.parseDouble((String) percentMap.get(meterIndex));

            //targetInterval: "month", "week", "day"
            //get the first and last reading of the interval for the past numberOfIntervals intervals
            List<Map<String, Object>> meterConsumptionHistoryList = new ArrayList<>();
            for(int i = 0; i < numberOfIntervals; i++) {
                String startDatetimeStr2 = "";
                String endDatetimeStr2 = "";

                String firstReadingTimeStr = "-";
                String lastReadingTimeStr = "-";
                String firstReadingVal = "-";
                String lastReadingVal = "-";
                String usage = "-";

                LocalDateTime endDatetime = DateTimeUtil.getLocalDateTime(endDatetimeStr);
                LocalDateTime endDatetimeMonthEnd = endDatetime.withDayOfMonth(endDatetime.getMonth().maxLength()).withHour(23).withMinute(59).withSecond(59);
                if(targetInterval.equalsIgnoreCase("month")){
                    LocalDateTime localNow = localHelper.getLocalNow();
                    LocalDateTime endDateTimeInterval = endDatetimeMonthEnd.minusMonths(i);
                    //skip the current month
                    if(endDateTimeInterval.getMonthValue() == localNow.getMonthValue() && endDateTimeInterval.getYear() == localNow.getYear()){
                        continue;
                    }
                    LocalDateTime startDateTimeInterval = endDatetimeMonthEnd.minusMonths(i+1);

                    String mbrAdditionalConstraint = "";
                    if(itemTypeEnum == ItemTypeEnum.METER_IWOW){
                        mbrAdditionalConstraint = " duplicated != true AND interpolated != true ";
                    }

                    Map<String, Object> resultMonthly = findMonthlyReading(
//                            startDateTimeInterval.toString(),
                            null,
                            mbrRangeHours,
                            endDateTimeInterval.toString(),
                            meterId,
                            itemReadingTableName,
                            itemReadingIndexColName,
                            itemReadingIdColName, //itemIdColName,
                            timeKey, valKey,
                            mbrAdditionalConstraint);
                    if(resultMonthly.containsKey("error")){
                        logger.info("error: " + resultMonthly.get("error"));
                        return Collections.singletonMap("error", resultMonthly.get("error"));
                    }
                    if(resultMonthly.containsKey("info")){
                        logger.info("info: " + resultMonthly.get("info"));
                        continue;
                    }

                    firstReadingVal = (String) resultMonthly.get("first_reading_val");
                    lastReadingVal = (String) resultMonthly.get("last_reading_val");
                    firstReadingTimeStr = ((String) resultMonthly.get("first_reading_time")).isEmpty() ? "-" : (String) resultMonthly.get("first_reading_time");
                    lastReadingTimeStr = ((String) resultMonthly.get("last_reading_time")).isEmpty() ? "-" : (String) resultMonthly.get("last_reading_time");
                    Double firstReadingValDouble = firstReadingVal.isEmpty()? null : Double.parseDouble(firstReadingVal);
                    Double lastReadingValDouble = lastReadingVal.isEmpty()? null: Double.parseDouble(lastReadingVal);
                    //round to 2 decimals
                    Double firstReadingValDouble2 = firstReadingValDouble==null? null : MathUtil.setDecimalPlaces(firstReadingValDouble, 2, RoundingMode.HALF_UP);
                    Double lastReadingValDouble2 = lastReadingValDouble==null? null: MathUtil.setDecimalPlaces(lastReadingValDouble, 2, RoundingMode.HALF_UP);
                    Double usageDouble = firstReadingValDouble2==null || lastReadingValDouble2==null ? null : lastReadingValDouble2 - firstReadingValDouble2;
//                Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                    firstReadingVal = firstReadingValDouble2==null? "-" : String.format("%.2f", firstReadingValDouble2);
                    lastReadingVal = lastReadingValDouble2==null? "-" : String.format("%.2f", lastReadingValDouble2);
                    usage = usageDouble==null? "-" : String.format("%.2f", usageDouble);

                }else {
                    //from endDatetimeStr, go back numberOfIntervals intervals
                    if (targetInterval.equalsIgnoreCase("month")) {
                        startDatetimeStr2 = "date_trunc('month', '" + endDatetimeStr + "'::timestamp - INTERVAL '" + (i+1) + " month')";
                        endDatetimeStr2 = "date_trunc('month', '" + endDatetimeStr + "'::timestamp - INTERVAL '" + i + " month')";
                    } else if (targetInterval.equalsIgnoreCase("week")) {
                        startDatetimeStr2 = "date_trunc('week', '" + endDatetimeStr + "'::timestamp - INTERVAL '" + (i+1) + " week')";
                        endDatetimeStr2 = "date_trunc('week', '" + endDatetimeStr + "'::timestamp - INTERVAL '" + i + " week')";
                    } else if (targetInterval.equalsIgnoreCase("day")) {
                        startDatetimeStr2 = "date_trunc('day', '" + endDatetimeStr + "'::timestamp - INTERVAL '" + (i+1) + " day')";
                        endDatetimeStr2 = "date_trunc('day', '" + endDatetimeStr + "'::timestamp - INTERVAL '" + i + " day')";
                    } else {
                        logger.info("invalid targetInterval: " + targetInterval);
                        return Collections.singletonMap("error", "invalid targetInterval: " + targetInterval);
                    }

                    String sql = "SELECT DISTINCT" +
                            " FIRST_VALUE(" + valKey + ") OVER w AS first_reading_val," +
                            " LAST_VALUE(" + valKey + ") OVER w AS last_reading_val," +
                            " FIRST_VALUE(" + timeKey + ") OVER w AS first_reading_time," +
                            " LAST_VALUE(" + timeKey + ") OVER w AS last_reading_time " +
                            " FROM " + itemReadingTableName + " WHERE " +
                            itemIdColName + " = '" + meterId + "' AND " +
                            timeKey + " BETWEEN " + startDatetimeStr2 + " AND " + endDatetimeStr2 +
                            " WINDOW w AS ( ORDER BY " + timeKey + " RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";

                    List<Map<String, Object>> resp2;
                    try {
                        resp2 = oqgHelper.OqgR2(sql, true);
                    } catch (Exception e) {
                        logger.info("oqgHelper error: " + e.getMessage());
                        return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
                    }
                    if (resp2 == null) {
                        logger.info("oqgHelper error: resp is null");
                        return Collections.singletonMap("error", "oqgHelper error: resp is null");
                    }
                    if (resp2.isEmpty()) {
                        logger.info("no record found");
//                    return Collections.singletonMap("info", "no meter found");
                        break;
                    }

                    firstReadingVal = (String) resp2.getFirst().get("first_reading_val");
                    lastReadingVal = (String) resp2.getFirst().get("last_reading_val");
                    firstReadingTimeStr = (String) resp2.getFirst().get("first_reading_time");
                    lastReadingTimeStr = (String) resp2.getFirst().get("last_reading_time");
                    Double firstReadingValDouble = firstReadingVal.isEmpty()? null : Double.parseDouble(firstReadingVal);
                    Double lastReadingValDouble = lastReadingVal.isEmpty()? null: Double.parseDouble(lastReadingVal);
                    //round to 2 decimals
                    Double firstReadingValDouble2 = firstReadingValDouble==null? null : MathUtil.setDecimalPlaces(firstReadingValDouble, 2, RoundingMode.HALF_UP);
                    Double lastReadingValDouble2 = lastReadingValDouble==null? null: MathUtil.setDecimalPlaces(lastReadingValDouble, 2, RoundingMode.HALF_UP);
                    Double usageDouble = firstReadingValDouble2==null || lastReadingValDouble2==null ? null : lastReadingValDouble2 - firstReadingValDouble2;
//                Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                    firstReadingVal = firstReadingValDouble2==null? "-" : String.format("%.2f", firstReadingValDouble2);
                    lastReadingVal = lastReadingValDouble2==null? "-" : String.format("%.2f", lastReadingValDouble2);
                    usage = usageDouble==null? "-" : String.format("%.2f", usageDouble);
                }

                Map<String, Object> usageHistory = new HashMap<>();

                String consolidatedTimeLabel = "";

                if("-".equals(firstReadingTimeStr) || "-".equals(lastReadingTimeStr)){
                    continue;
                }

                LocalDateTime firstReadingTime = DateTimeUtil.getLocalDateTime(firstReadingTimeStr);
                LocalDateTime lastReadingTime = DateTimeUtil.getLocalDateTime(lastReadingTimeStr);
                Duration duration = Duration.between(firstReadingTime, lastReadingTime);
                LocalDateTime middleReadingTime = firstReadingTime.plus(duration.dividedBy(2));
                String year = String.valueOf(middleReadingTime.getYear());
                String month = String.valueOf(middleReadingTime.getMonthValue());
                String week = String.valueOf(middleReadingTime.getDayOfYear() / 7);
                String day = String.valueOf(middleReadingTime.getDayOfMonth());
                if (targetInterval.equalsIgnoreCase("month")) {
                    consolidatedTimeLabel = year + "-" + month;
                } else if (targetInterval.equalsIgnoreCase("week")) {
                    consolidatedTimeLabel = year + "-" + week;
                } else if (targetInterval.equalsIgnoreCase("day")) {
                    consolidatedTimeLabel = year + "-" + month + "-" + day;
                }

                usageHistory.put("consolidated_time_label", consolidatedTimeLabel);
                usageHistory.put("first_reading_val", firstReadingVal);
                usageHistory.put("last_reading_val", lastReadingVal);
                usageHistory.put("first_reading_time", firstReadingTimeStr);
                usageHistory.put("last_reading_time", lastReadingTimeStr);
                usageHistory.put("usage", usage);

                meterConsumptionHistoryList.add(usageHistory);
            }
            meterListConsumptionHistory.add(Map.of(
                    "meter_id", meterId,
                    "meter_id_type", itemIdTypeStr,
                    "interval", targetInterval,
                    "meter_usage_history", meterConsumptionHistoryList,
                    "percentage", percentage
            ));
        }
        return Collections.singletonMap("meter_list_consolidated_usage_history", meterListConsumptionHistory);
    }

    public Map<String, Object> findMonthlyReading(/*String monthStartDatetimeStr, */
            LocalDateTime commissionedDatetime, Long mbrRangeHours,
            String monthEndDatetimeStr,
            String meterId,
            String itemReadingTableName,
            String itemReadingIndexColName,
            String itemReadingIdColName,
            String timeKey, String valKey,
            String mbrAdditionalConstraint) {
        logger.info("process findMonthlyReading");
        LocalDateTime searchingStart = localHelper.getLocalNow();

        LocalDateTime monthEndDatetimeDay = DateTimeUtil.getLocalDateTime(monthEndDatetimeStr);
        LocalDateTime monthEndDatetime =  monthEndDatetimeDay
                .withDayOfMonth(monthEndDatetimeDay.getMonth().maxLength())
                .withHour(23).withMinute(59).withSecond(59);
//        LocalDateTime monthStartDatetime = DateTimeUtil.getLocalDateTime(monthStartDatetimeStr);
        LocalDateTime monthStartDatetime = monthEndDatetime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);

        // find the 2 mbrs of the month
        Map<String, Object> result = new HashMap<>();
        String firstReadingTimestamp = "";
        String firstReadingVal = "";
        String lastReadingTimestamp = "";
        String lastReadingVal = "";

        // find the first reading of the month
        int theYear = monthEndDatetime.getYear();
        int theMonth = monthEndDatetime.getMonthValue();

        //first check commissionedDatetime
        int commissionedYear = 0;
        int commissionedMonth = 0;
        if(commissionedDatetime != null){
            commissionedYear = commissionedDatetime.getYear();
            commissionedMonth = commissionedDatetime.getMonthValue();
        }
        boolean useCommissionedDatetime = false;
        if(commissionedYear > 0){
            if(commissionedYear > theYear || (commissionedYear == theYear && commissionedMonth > theMonth)){
                // if commissionedDatetime is in the future, ignore the commissionedDatetime
                //return Collections.singletonMap("info", "commissionedDatetime is in the future");
            }
            if(commissionedYear==theYear && commissionedMonth==theMonth){
                //use the commissionedDatetime as the first reading of the month
                String firstReadingOfCurrentMonthSqlAsCommissionedMonth =
                        "SELECT " + valKey + ", " + timeKey + ", ref FROM " + itemReadingTableName
                                + " WHERE "
                                + itemReadingIdColName + " = '" + meterId
                                + "' AND " + timeKey + " >= '" + commissionedDatetime
                                + "' AND " + timeKey + " < '" + monthEndDatetime + "' "
//                        + AND " + " ref = 'mbr' "
                                + " ORDER BY " + timeKey + " LIMIT 1";
                List<Map<String, Object>> respCommissionedMonth;
                try {
                    respCommissionedMonth = oqgHelper.OqgR2(firstReadingOfCurrentMonthSqlAsCommissionedMonth, true);
                } catch (Exception e) {
                    logger.info("oqgHelper error: " + e.getMessage());
                    return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
                }
                if (respCommissionedMonth == null) {
                    logger.info("oqgHelper error: resp is null");
                    return Collections.singletonMap("error", "oqgHelper error: resp is null");
                }
                if (respCommissionedMonth.isEmpty()) {
                    logger.info("no commission month reading found for meter: " + meterId);
//                    return Collections.singletonMap("info", "no first reading of the month found for meter: " + meterId);
//                    result.put("first_reading_time", "-");
//                    result.put("first_reading_val", "-");
                }else {
                    firstReadingTimestamp = (String) respCommissionedMonth.getFirst().get(timeKey);
                    firstReadingVal = (String) respCommissionedMonth.getFirst().get(valKey);
                    useCommissionedDatetime = true;
                }
            }
        }

        if(firstReadingTimestamp.isEmpty()) {
            // search left 3 hours and right 3 hours of current month start
            // for the first reading with 'ref' as 'mbr',
            String firstReadingOfCurrentMonthSqlAsMbr =
                    "SELECT " + valKey + ", " + timeKey + ", ref FROM " + itemReadingTableName
                            + " WHERE "
                            + itemReadingIdColName + " = '" + meterId
                            + "' AND " + timeKey + " >= '" + monthStartDatetime.minusHours(mbrRangeHours)
                            + "' AND " + timeKey + " < '" + monthStartDatetime.plusHours(mbrRangeHours)
                            + "' AND " + " ref = 'mbr' "
                            + " ORDER BY " + timeKey + " LIMIT 1";
            List<Map<String, Object>> respStartSearchRange;
            try {
                respStartSearchRange = oqgHelper.OqgR2(firstReadingOfCurrentMonthSqlAsMbr, true);
            } catch (Exception e) {
                logger.info("oqgHelper error: " + e.getMessage());
                return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
            }
            if (respStartSearchRange == null) {
                logger.info("oqgHelper error: resp is null");
                return Collections.singletonMap("error", "oqgHelper error: resp is null");
            }
            if (respStartSearchRange.size() > 1) {
                logger.info("error: mbr count " + respStartSearchRange.size());
                return Collections.singletonMap("error", "mbr count " + respStartSearchRange.size() + " for meter: " + meterId);
            }
            // if mbr is found, use it
            if (respStartSearchRange.size() == 1) {
                firstReadingTimestamp = (String) respStartSearchRange.getFirst().get(timeKey);
                firstReadingVal = (String) respStartSearchRange.getFirst().get(valKey);
            }
            // if mbr near the beginning of the month is not found, use the first reading of the month
            // that is not duplicated and not interpolated
            if (respStartSearchRange.isEmpty()) {
                LocalDateTime beginOfMonth = monthStartDatetime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
                LocalDateTime endOfMonth = monthStartDatetime.withDayOfMonth(monthStartDatetime.getMonth().maxLength()).withHour(23).withMinute(59).withSecond(59);
                String firstReadingOfCurrentMonthSql = "SELECT " + itemReadingIndexColName + ", " + valKey + ", " + timeKey + ", ref FROM " + itemReadingTableName
                        + " WHERE " +
                        itemReadingIdColName + " = '" + meterId + "' AND " +
                        timeKey + " >= '" + beginOfMonth + "' AND " +
                        timeKey + " < '" + /*beginOfMonth.plusHours(3)*/ endOfMonth + "' ";

                if(mbrAdditionalConstraint != null && !mbrAdditionalConstraint.isEmpty()){
                    firstReadingOfCurrentMonthSql += " AND " + mbrAdditionalConstraint;
                }
                firstReadingOfCurrentMonthSql += " ORDER BY " + timeKey + " LIMIT 1";

                List<Map<String, Object>> respFirstReadingOfCurrentMonth;
                try {
                    respFirstReadingOfCurrentMonth = oqgHelper.OqgR2(firstReadingOfCurrentMonthSql, true);
                } catch (Exception e) {
                    logger.info("oqgHelper error: " + e.getMessage());
                    return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
                }
                if (respFirstReadingOfCurrentMonth == null) {
                    logger.info("oqgHelper error: resp is null");
                    return Collections.singletonMap("error", "oqgHelper error: resp is null");
                }
                if (respFirstReadingOfCurrentMonth.isEmpty()) {
                    logger.info("no first reading of the month found for meter: " + meterId);
//                    return Collections.singletonMap("info", "no first reading of the month found for meter: " + meterId);
//                    result.put("first_reading_time", "-");
//                    result.put("first_reading_val", "-");
                } else {
                    // update the first reading of the month to mbr if it is not
                    String firstReadingOfCurrentMonthRef = (String) respFirstReadingOfCurrentMonth.getFirst().get("ref");
                    if (firstReadingOfCurrentMonthRef == null || !firstReadingOfCurrentMonthRef.equalsIgnoreCase("mbr")) {
                        String firstReadingOfCurrentMonthId = (String) respFirstReadingOfCurrentMonth.getFirst().get(itemReadingIndexColName);
                        String updateFirstReadingOfCurrentMonthSql =
                                "UPDATE " + itemReadingTableName + " SET ref = 'mbr' WHERE "
                                        + itemReadingIndexColName + " = '" + firstReadingOfCurrentMonthId + "'";
                        try {
                            oqgHelper.OqgIU(updateFirstReadingOfCurrentMonthSql);
                            logger.info("updateFirstReadingOfCurrentMonthSql: " + updateFirstReadingOfCurrentMonthSql);
                        } catch (Exception e) {
                            logger.info("oqgHelper error: " + e.getMessage());
                            return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
                        }
                    }
                    firstReadingTimestamp = (String) respFirstReadingOfCurrentMonth.getFirst().get(timeKey);
                    firstReadingVal = (String) respFirstReadingOfCurrentMonth.getFirst().get(valKey);
                }
            }
        }

        // find the last reading of the month

        // search left 3 hours and right 3 hours of current month end
        String lastReadingOfCurrentMonthSqlAsMbr = "SELECT " + valKey + ", " + timeKey + ", ref FROM " + itemReadingTableName
                + " WHERE " +
                itemReadingIdColName + " = '" + meterId + "' AND " +
                timeKey + " >= '" + monthEndDatetime.minusHours(mbrRangeHours) + "' AND " +
                timeKey + " < '" + monthEndDatetime.plusHours(mbrRangeHours) + "' AND " +
                " ref = 'mbr' " +
                " ORDER BY " + timeKey + " LIMIT 1";
        List<Map<String, Object>> respEndSearchRange;
        try {
            respEndSearchRange = oqgHelper.OqgR2(lastReadingOfCurrentMonthSqlAsMbr, true);
        } catch (Exception e) {
            logger.info("oqgHelper error: " + e.getMessage());
            return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
        }
        if (respEndSearchRange == null) {
            logger.info("oqgHelper error: resp is null");
            return Collections.singletonMap("error", "oqgHelper error: resp is null");
        }
        if (respEndSearchRange.size() > 1) {
            logger.info("error: mbr count " + respEndSearchRange.size());
            return Collections.singletonMap("error", "mbr count " + respEndSearchRange.size());
        }
        // if mbr is found, use it
        if (respEndSearchRange.size() == 1) {
            lastReadingTimestamp = (String) respEndSearchRange.getFirst().get(timeKey);
            lastReadingVal = (String) respEndSearchRange.getFirst().get(valKey);
        }
        // if mbr is not found, use the first reading of the following month
        // that is not duplicated and not interpolated
        if(respEndSearchRange.isEmpty()) {
            LocalDateTime beginOfFollowingMonth = monthEndDatetime.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
            String firstReadingOfFollowingMonthSql = "SELECT " + itemReadingIndexColName + ", " + valKey + ", " + timeKey + ", ref FROM "
                    + itemReadingTableName + " WHERE " +
                    itemReadingIdColName + " = '" + meterId + "' AND " +
                    timeKey + " >= '" + beginOfFollowingMonth + "' AND " +
                    timeKey + " < '" + beginOfFollowingMonth.plusHours(mbrRangeHours) + "' ";

            if(mbrAdditionalConstraint != null && !mbrAdditionalConstraint.isEmpty()){
                firstReadingOfFollowingMonthSql += " AND " + mbrAdditionalConstraint;
            }
            firstReadingOfFollowingMonthSql += " ORDER BY " + timeKey + " LIMIT 1";

            List<Map<String, Object>> respFirstReadingOfFollowingMonth;
            try {
                respFirstReadingOfFollowingMonth = oqgHelper.OqgR2(firstReadingOfFollowingMonthSql, true);
            } catch (Exception e) {
                logger.info("oqgHelper error: " + e.getMessage());
                return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
            }
            if (respFirstReadingOfFollowingMonth == null) {
                logger.info("oqgHelper error: resp is null");
                return Collections.singletonMap("error", "oqgHelper error: resp is null");
            }
            if (respFirstReadingOfFollowingMonth.isEmpty()) {
                logger.info("no first reading of the following month found");
//                return Collections.singletonMap("info", "no first reading of the following month found");
//                lastReadingTimestamp = "-";
//                lastReadingVal = "-";
            }else{
                String respFirstReadingOfFollowingMonthRef = (String) respFirstReadingOfFollowingMonth.getFirst().get("ref");
                if(respFirstReadingOfFollowingMonthRef == null || !respFirstReadingOfFollowingMonthRef.equalsIgnoreCase("mbr")) {
                    // update the first reading of the following month to mbr
                    String firstReadingOfFollowingMonthId = (String) respFirstReadingOfFollowingMonth.getFirst().get(itemReadingIndexColName);
                    String updateFirstReadingOfFollowingMonthSql =
                            "UPDATE " + itemReadingTableName + " SET ref = 'mbr' WHERE "
                                    + itemReadingIndexColName + " = '" + firstReadingOfFollowingMonthId + "'";
                    try {
                        oqgHelper.OqgIU(updateFirstReadingOfFollowingMonthSql);
                        logger.info("updateFirstReadingOfFollowingMonthSql: " + updateFirstReadingOfFollowingMonthSql);
                    } catch (Exception e) {
                        logger.info("oqgHelper error: " + e.getMessage());
                        return Collections.singletonMap("error", "oqgHelper error: " + e.getMessage());
                    }
                }
                lastReadingTimestamp = (String) respFirstReadingOfFollowingMonth.getFirst().get(timeKey);
                lastReadingVal = (String) respFirstReadingOfFollowingMonth.getFirst().get(valKey);
            }
        }

        result.put("first_reading_time", firstReadingTimestamp);
        result.put("first_reading_val", firstReadingVal);
        result.put("last_reading_time", lastReadingTimestamp);
        result.put("last_reading_val", lastReadingVal);
        result.put("use_commissioned_datetime", useCommissionedDatetime);

//        LocalDateTime searchingEnd = localHelper.getLocalNow();
//        logger.info("findMonthlyReading duration: " + Duration.between(searchingStart, searchingEnd).toSeconds() + " seconds");

        return result;
    }
}

