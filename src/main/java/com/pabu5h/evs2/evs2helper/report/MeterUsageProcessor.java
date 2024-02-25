package com.pabu5h.evs2.evs2helper.report;

import com.pabu5h.evs2.dto.ItemIdTypeEnum;
import com.pabu5h.evs2.dto.ItemTypeEnum;
import com.pabu5h.evs2.evs2helper.locale.LocalHelper;
import com.pabu5h.evs2.evs2helper.scope.ScopeHelper;
import com.pabu5h.evs2.oqghelper.OqgHelper;
import com.pabu5h.evs2.oqghelper.QueryHelper;
import com.xt.utils.DateTimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    public Map<String, Object> getMeterListUsageSummary(Map<String, String> request) {
        logger.info("process getMeterListUsageSummary");

        String projectScope = request.get("project_scope");
        String siteScope = request.get("site_scope");
        String meterSelectSql = request.get("id_select_query");
        String startDatetimeStr = request.get("start_datetime");
        String endDatetimeStr = request.get("end_datetime");
        String itemIdTypeStr = request.get("item_id_type")==null?"":request.get("item_id_type");
//        ItemIdTypeEnum itemIdType = itemIdTypeStr.isEmpty()? null : ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
        String isMonthlyStr = request.get("is_monthly");
        int testCount = Integer.parseInt(request.getOrDefault("test_count", "0"));

        boolean isMonthly = false;
        if (isMonthlyStr != null && !isMonthlyStr.isEmpty()) {
            isMonthly = Boolean.parseBoolean(isMonthlyStr);
        }

        String meterTypeStr = request.get("item_type");
        ItemTypeEnum meterTypeEnum = null;
        if(meterTypeStr != null) {
            meterTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig(projectScope, itemIdTypeStr);
        if(meterTypeEnum==null){
            meterTypeEnum = ItemTypeEnum.valueOf((String)itemConfig.get("itemTypeEnum"));
        }
        String targetReadingTableName = (String) itemConfig.get("targetReadingTableName");
        String targetTableName = (String) itemConfig.get("targetTableName");
        String itemIdColName = (String) itemConfig.get("itemIdColName");
        String itemSnColName = (String) itemConfig.get("itemSnColName");
        String itemNameColName = (String) itemConfig.get("itemNameColName");
        String itemAltName = (String) itemConfig.get("itemAltNameColName");
        String panelTagColName = (String) itemConfig.get("panelTagColName");
        String itemIdColSel = (String) itemConfig.get("itemIdColSel");
        String itemLocColSel = (String) itemConfig.get("itemLocColSel");
        String timeKey =(String) itemConfig.get("timeKey");
        String valKey = (String) itemConfig.get("valKey");

        String sortBy = request.get("sort_by");
        String sortOrder = request.get("sort_order");

        Map<String, Object> result = new HashMap<>();

        int limit = Integer.parseInt(request.getOrDefault("max_rows_per_page", "20"));
        int page = Integer.parseInt(request.getOrDefault("current_page", "1"));
        int offset = (page - 1) * limit;
        String getCount = request.get("get_count");
        boolean getCountBool = true;
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
        if(!meterSelectSql2.contains("commissioned_timestamp")) {
            meterSelectSql2 += " , commissioned_timestamp";
        }
        meterSelectSql2 += " ORDER BY " + itemIdColName + " LIMIT " + limit + " OFFSET " + offset;

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

        List<Map<String, Object>> usageSummaryList = new ArrayList<>();
        int processedCount = 0;
        for (Map<String, Object> meterMap : resp) {
            String meterId = (String) meterMap.get(itemIdColName);
            String meterSn = meterMap.get(itemSnColName) == null ? "" : (String) meterMap.get(itemSnColName);
            String meterName = meterMap.get(itemNameColName) == null ? "" : (String) meterMap.get(itemNameColName);
            String meterAltName = meterMap.get(itemAltName) == null ? "" : (String) meterMap.get(itemAltName);
            String commissionedTimestampStr = meterMap.get("commissioned_timestamp") == null ? "" : (String) meterMap.get("commissioned_timestamp");
            LocalDateTime commissionedDatetime = DateTimeUtil.getLocalDateTime(commissionedTimestampStr);
            Integer commissionedYear = null;
            Integer commissionedMonth = null;
            if(commissionedDatetime != null){
                commissionedYear = commissionedDatetime.getYear();
                commissionedMonth = commissionedDatetime.getMonthValue();
            }

            if (meterId == null || meterId.isEmpty()) {
                logger.info("meterId is null or empty");
                continue;
            }

//            if(!meterId.equals("E019")){
//                continue;
//            }

            if (isMonthly) {
                Map<String, Object> resultMonthly =
                    findMonthlyReading(
                        commissionedDatetime,
//                        startDatetimeStr,
                        endDatetimeStr,
                        meterId,
                        targetReadingTableName,
                        itemIdColName,
                        timeKey, valKey);

                if (resultMonthly.containsKey("error")) {
                    logger.info("error: " + resultMonthly.get("error"));
                    return Collections.singletonMap("error", "error: " + resultMonthly.get("error"));
                }
                if (resultMonthly.containsKey("info")) {
                    logger.info("info: " + resultMonthly.get("info"));
//                    return Collections.singletonMap("info", "info: " + resultMonthly.get("info"));
                    continue;
                }
                LinkedHashMap<String, Object> usageSummary = new LinkedHashMap<>();
//                usageSummary.put(itemSnColName, meterSn);
//                usageSummary.put(itemNameColName, meterName);

//                if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
//                    usageSummary.put("alt_name", meterAltName);
//                }else if(meterTypeEnum == ItemTypeEnum.METER_3P){
//                    usageSummary.put("panel_tag", meterMap.get("panel_tag"));
//                }
//                usageSummary.put("alt_name", meterAltName);
                String[] idColList = itemIdColSel.split(",");
                String[] locColList = itemLocColSel.split(",");
                for(String idCol : idColList){
                    usageSummary.put(idCol, meterMap.get(idCol));
                }
                for(String locCol : locColList){
                    usageSummary.put(locCol, meterMap.get(locCol));
                }
                usageSummary.put("site_tag", meterMap.get("site_tag"));
                if(meterTypeEnum == ItemTypeEnum.METER_IWOW){
                    usageSummary.put("meter_type", meterMap.get("meter_type"));
                }
                String firstReadingVal = (String) resultMonthly.get("first_reading_val");
                String lastReadingVal = (String) resultMonthly.get("last_reading_val");
                String firstReadingTime = (String) resultMonthly.get("first_reading_time");
                String lastReadingTime = (String) resultMonthly.get("last_reading_time");
                Double firstReadingValDouble = Double.parseDouble(firstReadingVal);
                Double lastReadingValDouble = Double.parseDouble(lastReadingVal);
                Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                firstReadingVal = String.format("%.2f", firstReadingValDouble);
                lastReadingVal = String.format("%.2f", lastReadingValDouble);
                String usage = String.format("%.2f", usageDouble);

                usageSummary.put("first_reading_time", firstReadingTime);
                usageSummary.put("last_reading_time", lastReadingTime);
                usageSummary.put("first_reading_val", firstReadingVal);
                usageSummary.put("last_reading_val", lastReadingVal);
                usageSummary.put("usage", usage);
//                usageSummary.put("first_reading_ref", resultMonthly.get("first_reading_ref"));
//                usageSummary.put("last_reading_ref", resultMonthly.get("last_reading_ref"));

                usageSummaryList.add(usageSummary);

                processedCount++;
                if(testCount > 0 && processedCount >= testCount){
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
                    " FROM " + targetReadingTableName + " WHERE " +
                    itemIdColName + " = '" + meterId + "' AND " +
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
                Double firstReadingValDouble = Double.parseDouble(firstReadingVal);
                Double lastReadingValDouble = Double.parseDouble(lastReadingVal);
                Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                firstReadingVal = String.format("%.2f", firstReadingValDouble);
                lastReadingVal = String.format("%.2f", lastReadingValDouble);
                usage = String.format("%.2f", usageDouble);
            }
            LinkedHashMap<String, Object> usageSummary = new LinkedHashMap<>();

            String[] idColList = itemIdColSel.split(",");
            String[] locColList = itemLocColSel.split(",");
            for(String idCol : idColList){
                usageSummary.put(idCol, meterMap.get(idCol));
            }
            for(String locCol : locColList){
                usageSummary.put(locCol, meterMap.get(locCol));
            }

            Double firstReadingValDouble = Double.parseDouble(firstReadingVal);
            Double lastReadingValDouble = Double.parseDouble(lastReadingVal);
            Double usageDouble = lastReadingValDouble - firstReadingValDouble;
            firstReadingVal = String.format("%.2f", firstReadingValDouble);
            lastReadingVal = String.format("%.2f", lastReadingValDouble);
            usage = String.format("%.2f", usageDouble);

            usageSummary.put("first_reading_time", firstReadingTime);
            usageSummary.put("last_reading_time", lastReadingTime);
            usageSummary.put("first_reading_val", firstReadingVal);
            usageSummary.put("last_reading_val", lastReadingVal);
            usageSummary.put("usage", usage);

            if(commissionedDatetime != null){
                result.put("commissioned_timestamp", commissionedTimestampStr);
            }

            usageSummaryList.add(usageSummary);

            processedCount++;
            if(testCount > 0 && processedCount >= testCount){
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

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig(projectScope, itemIdTypeStr);
        String targetReadingTableName = (String) itemConfig.get("targetReadingTableName");
        String targetTableName = (String) itemConfig.get("targetTableName");
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
            if (itemIdTypeEnum != ItemIdTypeEnum.NAME && itemIdTypeEnum != ItemIdTypeEnum.SN) {
                meterIdList2.clear();
                for (String meterId : meterIdList) {
                    String meterIdColName = "";
                    if (itemIdTypeEnum == ItemIdTypeEnum.ALT_NAME) {
                        meterIdColName = "alt_name";
                    }
                    Map<String, Object> result = queryHelper.getTableField(
                            "meter_iwow", "item_sn", meterIdColName, meterId);
                    String meter_id = (String) result.get("item_sn");
                    if (meter_id != null && !meter_id.isEmpty()) {
                        meterIdList2.add(meter_id);
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
                    Map<String, Object> resultMonthly = findMonthlyReading(
//                            startDateTimeInterval.toString(),
                            null,
                            endDateTimeInterval.toString(),
                            meterId,
                            targetReadingTableName,
                            itemIdColName,
                            timeKey, valKey);
                    if(resultMonthly.containsKey("error")){
                        logger.info("error: " + resultMonthly.get("error"));
                        return Collections.singletonMap("error", resultMonthly.get("error"));
                    }
                    if(resultMonthly.containsKey("info")){
                        logger.info("info: " + resultMonthly.get("info"));
                        continue;
                    }

                    firstReadingTimeStr = (String) resultMonthly.get("first_reading_time");
                    lastReadingTimeStr = (String) resultMonthly.get("last_reading_time");
                    firstReadingVal = (String) resultMonthly.get("first_reading_val");
                    lastReadingVal = (String) resultMonthly.get("last_reading_val");
                    Double firstReadingValDouble = Double.parseDouble(firstReadingVal);
                    Double lastReadingValDouble = Double.parseDouble(lastReadingVal);
                    Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                    firstReadingVal = String.format("%.2f", firstReadingValDouble);
                    lastReadingVal = String.format("%.2f", lastReadingValDouble);
                    usage = String.format("%.2f", usageDouble);

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
                            " FROM " + targetReadingTableName + " WHERE " +
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
                    firstReadingTimeStr = (String) resp2.getFirst().get("first_reading_time");
                    lastReadingTimeStr = (String) resp2.getFirst().get("last_reading_time");
                    firstReadingVal = (String) resp2.getFirst().get("first_reading_val");
                    lastReadingVal = (String) resp2.getFirst().get("last_reading_val");
                    Double firstReadingValDouble = Double.parseDouble(firstReadingVal);
                    Double lastReadingValDouble = Double.parseDouble(lastReadingVal);
                    Double usageDouble = lastReadingValDouble - firstReadingValDouble;
                    firstReadingVal = String.format("%.2f", firstReadingValDouble);
                    lastReadingVal = String.format("%.2f", lastReadingValDouble);
                    usage = String.format("%.2f", usageDouble);
                }

                Map<String, Object> usageHistory = new HashMap<>();

                String consolidatedTimeLabel = "";

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
                    "meter_usage_history", meterConsumptionHistoryList
            ));
        }
        return Collections.singletonMap("meter_list_consolidated_usage_history", meterListConsumptionHistory);
    }

    Map<String, Object> findMonthlyReading(/*String monthStartDatetimeStr, */
            LocalDateTime commissionedDatetime,
            String monthEndDatetimeStr,
            String meterId,
            String targetReadingTableName,
            String itemIdColName,
            String timeKey, String valKey) {

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
        if(commissionedYear > 0){
            if(commissionedYear > theYear || (commissionedYear == theYear && commissionedMonth > theMonth)){
                // if commissionedDatetime is in the future, ignore the commissionedDatetime
                //return Collections.singletonMap("info", "commissionedDatetime is in the future");
            }
            if(commissionedYear==theYear && commissionedMonth==theMonth){
                //use the commissionedDatetime as the first reading of the month
                String firstReadingOfCurrentMonthSqlAsCommissionedMonth =
                    "SELECT " + valKey + ", " + timeKey + ", ref FROM " + targetReadingTableName
                    + " WHERE "
                    + itemIdColName + " = '" + meterId
                    + "' AND " + timeKey + " >= '" + commissionedDatetime
                    + "' AND " + timeKey + " < '" + monthEndDatetime
//                        + "' AND " + " ref = 'mbr' "
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
                    logger.info("no first reading of the month found for meter: " + meterId);
                    return Collections.singletonMap("info", "no first reading of the month found for meter: " + meterId);
                }
                firstReadingTimestamp = (String) respCommissionedMonth.getFirst().get(timeKey);
            }
        }

        if(firstReadingTimestamp.isEmpty()) {
            // search left 3 hours and right 3 hours of current month start
            // for the first reading with 'ref' as 'mbr',
            String firstReadingOfCurrentMonthSqlAsMbr =
                    "SELECT " + valKey + ", " + timeKey + ", ref FROM " + targetReadingTableName
                    + " WHERE "
                    + itemIdColName + " = '" + meterId
                    + "' AND " + timeKey + " >= '" + monthStartDatetime.minusHours(3)
                    + "' AND " + timeKey + " < '" + monthStartDatetime.plusHours(3)
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
            if (respStartSearchRange.isEmpty()) {
                LocalDateTime beginOfMonth = monthStartDatetime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
                LocalDateTime endOfMonth = monthStartDatetime.withDayOfMonth(monthStartDatetime.getMonth().maxLength()).withHour(23).withMinute(59).withSecond(59);
                String firstReadingOfCurrentMonthSql = "SELECT id, " + valKey + ", " + timeKey + ", ref FROM " + targetReadingTableName
                        + " WHERE " +
                        itemIdColName + " = '" + meterId + "' AND " +
                        timeKey + " >= '" + beginOfMonth + "' AND " +
                        timeKey + " < '" + /*beginOfMonth.plusHours(3)*/ endOfMonth + "' " +
                        " ORDER BY " + timeKey + " LIMIT 1";
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
                    return Collections.singletonMap("info", "no first reading of the month found for meter: " + meterId);
                } else {
                    // update the first reading of the month to mbr if it is not
                    String firstReadingOfCurrentMonthRef = (String) respFirstReadingOfCurrentMonth.getFirst().get("ref");
                    if (firstReadingOfCurrentMonthRef == null || !firstReadingOfCurrentMonthRef.equalsIgnoreCase("mbr")) {
                        String firstReadingOfCurrentMonthId = (String) respFirstReadingOfCurrentMonth.getFirst().get("id");
                        String updateFirstReadingOfCurrentMonthSql =
                                "UPDATE " + targetReadingTableName + " SET ref = 'mbr' WHERE " +
                                        "id = '" + firstReadingOfCurrentMonthId + "'";
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
        String lastReadingOfCurrentMonthSqlAsMbr = "SELECT " + valKey + ", " + timeKey + ", ref FROM " + targetReadingTableName
                + " WHERE " +
                itemIdColName + " = '" + meterId + "' AND " +
                timeKey + " >= '" + monthEndDatetime.minusHours(3) + "' AND " +
                timeKey + " < '" + monthEndDatetime.plusHours(3) + "' AND " +
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
        if(respEndSearchRange.isEmpty()) {
            LocalDateTime beginOfFollowingMonth = monthEndDatetime.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
            String firstReadingOfFollowingMonthSql = "SELECT id, " + valKey + ", " + timeKey + ", ref FROM "
                    + targetReadingTableName + " WHERE " +
                    itemIdColName + " = '" + meterId + "' AND " +
                    timeKey + " >= '" + beginOfFollowingMonth + "' AND " +
                    timeKey + " < '" + beginOfFollowingMonth.plusHours(3) + "' " +
                    " ORDER BY " + timeKey + " LIMIT 1";
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
                return Collections.singletonMap("info", "no first reading of the following month found");
            }else{
                String respFirstReadingOfFollowingMonthRef = (String) respFirstReadingOfFollowingMonth.getFirst().get("ref");
                if(respFirstReadingOfFollowingMonthRef == null || !respFirstReadingOfFollowingMonthRef.equalsIgnoreCase("mbr")) {
                    // update the first reading of the following month to mbr
                    String firstReadingOfFollowingMonthId = (String) respFirstReadingOfFollowingMonth.getFirst().get("id");
                    String updateFirstReadingOfFollowingMonthSql =
                            "UPDATE " + targetReadingTableName + " SET ref = 'mbr' WHERE " +
                                    "id = '" + firstReadingOfFollowingMonthId + "'";
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
        return result;
    }
}
