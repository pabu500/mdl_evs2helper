package com.pabu5h.evs2.evs2helper.report;

import com.pabu5h.evs2.dto.ItemTypeEnum;
import com.pabu5h.evs2.evs2helper.locale.LocalHelper;
import com.pabu5h.evs2.evs2helper.scope.ScopeHelper;
import com.pabu5h.evs2.oqghelper.OqgHelper;

import com.xt.utils.DateTimeUtil;
import com.xt.utils.MathUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;

@Component
public class PmtUsageReport {
    Logger logger = Logger.getLogger(PmtUsageReport.class.getName());

    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private LocalHelper localHelper;
    @Autowired
    private ScopeHelper scopeHelper;
    @Autowired
    private MeterUsageProcessor meterUsageProcessor;
    @Autowired
    private ReportHelper reportHelper;

    public Map<String, Object> generatePmtUsageReport(String jobTypeName, Map<String, String> request, List<Map<String, Object>> meterList) {
        logger.info("process generatePmtUsageReport");

        String isMonthlyStr = request.get("is_monthly") == null ? "false" : request.get("is_monthly");
        boolean isMonthly = Boolean.parseBoolean(isMonthlyStr);
        if(!isMonthly){
            return Collections.singletonMap("error", "Only monthly report is supported");
        }

        String meterTypeStr = request.get("item_type");
        ItemTypeEnum meterTypeEnum = null;
        if(meterTypeStr != null) {
            meterTypeEnum = ItemTypeEnum.valueOf(meterTypeStr.toUpperCase());
        }

        String startDatetimeStr = request.get("from_timestamp");
        String endDatetimeStr = request.get("to_timestamp");

        String projectScope = request.get("project_scope");
        String siteScope = request.get("site_scope");

        String peakFromStr = request.get("peak_from");
        String peakToStr = request.get("peak_to");
        String[] peakFrom = peakFromStr.split(":");
        String[] peakTo = peakToStr.split(":");
        String peakFromHourStr = peakFrom[0];
        String peakFromMinuteStr = peakFrom[1];
        String peakToHourStr = peakTo[0];
        String peakToMinuteStr = peakTo[1];
        int peakFromHour = Integer.parseInt(peakFromHourStr);
        int peakFromMinute = Integer.parseInt(peakFromMinuteStr);
        int peakToHour = Integer.parseInt(peakToHourStr);
        int peakToMinute = Integer.parseInt(peakToMinuteStr);

        int dominateIntervalMinutes = 30;

        Map<String, Object> usageSummary = meterUsageProcessor.getMeterListUsageSummary(request, meterList);

        Map<String, Object> itemConfig = scopeHelper.getItemTypeConfig(projectScope, "");
        String targetReadingTableName = (String) itemConfig.get("targetReadingTableName");
        String targetTableName = (String) itemConfig.get("targetTableName");
        String itemIdColName = (String) itemConfig.get("itemIdColName");
        String itemSnColName = (String) itemConfig.get("itemSnColName");
        String itemIdColSel = (String) itemConfig.get("itemIdColSel");
        String itemLocColSel = (String) itemConfig.get("itemLocColSel");

        List<Map<String, Object>> meterListUsageSummary = (List<Map<String, Object>>) usageSummary.get("meter_list_usage_summary");
        for(Map<String, Object> meterUsageSummary : meterListUsageSummary){
            String firstReadTimeStr = (String) meterUsageSummary.get("first_reading_time");
            String lastReadTimeStr = (String) meterUsageSummary.get("last_reading_time");
            if(firstReadTimeStr == null || lastReadTimeStr == null){
                logger.warning("missing first or last read time");
                continue;
            }
            LocalDateTime firstReadTime = DateTimeUtil.getLocalDateTime(firstReadTimeStr);
            LocalDateTime lastReadTime = DateTimeUtil.getLocalDateTime(lastReadTimeStr);
            LocalDateTime midReadTime = firstReadTime.plus(Duration.between(firstReadTime, lastReadTime).dividedBy(2));
            LocalDateTime monthBegin = midReadTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            int daysInMonth = DateTimeUtil.getDaysInMonth(midReadTime);
            LocalDateTime firstReadTimeAdj = firstReadTime.minus(Duration.ofSeconds(3));
            LocalDateTime lastReadTimeAdj = lastReadTime.plus(Duration.ofSeconds(3));

            String sql = "select dt, a_imp, c_md_sb_a_imp, ref from " + targetReadingTableName
                    + " where " + itemIdColName + " = '" + meterUsageSummary.get(itemIdColName) + "' "
                    + " and dt >= '" + firstReadTimeAdj + "' and dt <= '" + lastReadTimeAdj + "'"
                    + " order by dt asc";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2(sql, true);
            } catch (Exception e) {
                logger.warning("failed to get meter reading: " + e.getMessage());
                continue;
            }

            Map<String, Object> populateResult = getPopulatePeriodList(
                    dominateIntervalMinutes,
                    resp,
                    monthBegin,
                    firstReadTime, lastReadTime,
                    daysInMonth,
                    peakFromHour, peakFromMinute, peakToHour, peakToMinute);
            List<Map<String, Object>> periodList = (List<Map<String, Object>>) populateResult.get("period_list");
            List<Map<String, Object>> peakList = (List<Map<String, Object>>) populateResult.get("peak_list");
            double maxDemand = (double) populateResult.get("max_demand");
            meterUsageSummary.put("period_list", periodList);
            meterUsageSummary.put("peak_list", peakList);
            meterUsageSummary.put("max_demand", maxDemand);
        }

        Map<String, Object> mergedResult = mergePeriodList(meterListUsageSummary);
        meterListUsageSummary.add(Map.of(
                "is_merged", true,
                "period_list", mergedResult.get("merged_list"),
                "peak_list", new ArrayList<>()
                ));


        List<Map<String, Object>> multiSheetReportInfo = new ArrayList<>();
        double mergedTotalUsage = 0;
        double mergedPeakUsage = 0;
        double mergedOffPeakUsage = 0;
        double mergedMaxDemand = 0;
        for(Map<String, Object> meterUsageDetail : meterListUsageSummary){
            String meterId = (String) meterUsageDetail.get(itemIdColName);
            String meterSn = (String) meterUsageDetail.get(itemSnColName);
            if(meterTypeEnum == ItemTypeEnum.METER_3P){
                meterId = (String) meterUsageDetail.get("panel_tag");
            }

            boolean isMerged = meterUsageDetail.get("is_merged") != null && (boolean) meterUsageDetail.get("is_merged");
            List<Map<String, Object>> periodList = (List<Map<String, Object>>) meterUsageDetail.get("period_list");
            List<Map<String, Object>> peakList = (List<Map<String, Object>>) meterUsageDetail.get("peak_list");

            boolean missingData = false;
            int periodDays = periodList.size()/(24 * 60 / dominateIntervalMinutes);
            int peakDays = peakList.size();
            if(peakDays != periodDays /*&& !isMerged*/){
                missingData = true;
            }

            Map<String, Object> usageStatResult = getUsageStat(periodList, peakList);
            double totalUsage = (double) usageStatResult.get("total_usage");
            double peakUsage = (double) usageStatResult.get("peak_usage");
            double offPeakUsage = totalUsage - peakUsage;
            double maxDemand = 0;
            if(!isMerged){
                maxDemand = (double) meterUsageDetail.get("max_demand");
                maxDemand = MathUtil.setDecimalPlaces(maxDemand, 2, RoundingMode.HALF_UP);
            }

            Map<String, Object> sheetInfo = new HashMap<>();
            String sheetName = meterId;
            if(isMerged) {
                sheetName = "Total";
            }

            mergedTotalUsage += totalUsage;
            mergedPeakUsage += peakUsage;
            mergedOffPeakUsage += offPeakUsage;
            //mergedMaxDemand = Math.max(mergedMaxDemand, maxDemand);
            mergedMaxDemand += maxDemand;
            mergedMaxDemand = MathUtil.setDecimalPlaces(mergedMaxDemand, 2, RoundingMode.HALF_UP);

            sheetInfo.put("sheet_name", sheetName);
            sheetInfo.put("report", periodList);

            LinkedHashMap<String, Integer> headerMap = new LinkedHashMap<>();

            headerMap.put("#", 600);
            headerMap.put("date", 5300);
            headerMap.put("period", 2400);
            if(!isMerged) {
                headerMap.put("kwh_total", 3400);
            }
            headerMap.put("kwh_diff", 3300);
            if(!isMerged) {
                headerMap.put("ref", 2100);
            }

            sheetInfo.put("header", headerMap);

            List<Map<String, Object>> pathMapList = new ArrayList<>();
            String col1 = "H";
            String col2 = "I";
            double keyWidth = 3800;
            if(!isMerged) {
                pathMapList.add(Map.of("cell", col1 + "2", "value", meterId == null ? "" : meterId, "width", keyWidth));
                pathMapList.add(Map.of("cell", col2 + "2", "value", meterSn == null ? "" : meterSn, "width", keyWidth));
            }
            pathMapList.add(Map.of("cell", col1+"3","value", "Total", "width", keyWidth, "style","key"));
            pathMapList.add(Map.of("cell", col2+"3","value", isMerged? mergedTotalUsage : totalUsage, "width", keyWidth));
            pathMapList.add(Map.of("cell", col1+"4","value", "Peak", "style","key"));
            pathMapList.add(Map.of("cell", col2+"4","value",missingData? "-" : isMerged? mergedPeakUsage : peakUsage));
            pathMapList.add(Map.of("cell", col1+"5","value", "Off Peak", "style","key"));
            pathMapList.add(Map.of("cell", col2+"5","value", missingData? "-" : isMerged? mergedOffPeakUsage : offPeakUsage));
            pathMapList.add(Map.of("cell", col1+"6","value", "Max Demand", "style","key"));
            pathMapList.add(Map.of("cell", col2+"6","value", isMerged? mergedMaxDemand : maxDemand));

            sheetInfo.put("patch", pathMapList);

            multiSheetReportInfo.add(sheetInfo);
        }

        Map<String, Object> result = reportHelper.genReportExcelMultiSheet(jobTypeName, multiSheetReportInfo);
        return result;
    }

    private Map<String, Object> mergePeriodList(List<Map<String, Object>> meterListUsageSummary){
        List<Map<String, Object>> mergedList = new ArrayList<>();
        int maxLength = 0;
        for(Map<String, Object> meterUsageDetail : meterListUsageSummary){
            List<Map<String, Object>> periodList = (List<Map<String, Object>>) meterUsageDetail.get("period_list");
            if(periodList.size() > maxLength){
                maxLength = periodList.size();
            }
        }
        for(int i = 0; i < maxLength; i++){
            LinkedHashMap<String, Object> row = new LinkedHashMap<>();
            row.put("fullIndex", i + 1);
            int j = 0;
            Double mergedKwh = null;
            for(Map<String, Object> meterUsageDetail : meterListUsageSummary){
                List<Map<String, Object>> periodList = (List<Map<String, Object>>) meterUsageDetail.get("period_list");

                if(i < periodList.size()){
                    Map<String, Object> period = periodList.get(i);
                    if(j == 0){
                        row.put("dtStr", period.get("dtStr"));
                        row.put("periodIndex", period.get("periodIndex"));
                        row.put("diff", period.get("diff"));

                        if(period.get("diff")!=null) {
                            if (!period.get("diff").toString().isEmpty() && !period.get("diff").toString().equals("-")) {
                                mergedKwh = MathUtil.ObjToDouble(period.get("diff"));
                            }
                        }
                    }else{
                        if(mergedKwh == null){
                            continue;
                        }

                        Double kwhDiff = null;
                        if(period.get("diff")!=null) {
                            if (!period.get("diff").toString().isEmpty() && !period.get("diff").toString().equals("-")) {
                                kwhDiff = MathUtil.ObjToDouble(period.get("diff"));
                            }
                            if (kwhDiff != null) {
                                mergedKwh += kwhDiff;
                            }else {
                                mergedKwh = null;
                            }
                        }
                    }
                }
                j++;
            }
            row.put("diff", mergedKwh == null? "-" : mergedKwh);
            mergedList.add(row);
        }
        return Map.of("merged_list", mergedList);
    }

    private Map<String, Object> getUsageStat(List<Map<String, Object>> periodList, List<Map<String, Object>> peakList){
        double firstReading = 0;
        double lastReading = 0;
        //find first and last reading from periodList
        for(Map<String, Object> period : periodList){
            Object aImpObj = period.get("a_imp");
            if(aImpObj== null){
                continue;
            }
            firstReading = MathUtil.ObjToDouble(period.get("a_imp"));
            break;
        }
        for(int i = periodList.size() - 1; i >= 0; i--){
            Map<String, Object> period = periodList.get(i);
            Object aImpObj = period.get("a_imp");
            if(aImpObj== null){
                continue;
            }
            lastReading= MathUtil.ObjToDouble(period.get("a_imp"));
            break;
        }
        double totalUsage = lastReading - firstReading;

        double peakUsage = 0;
        for(Map<String, Object> peak : peakList){
            double peakStartReading = MathUtil.ObjToDouble(peak.get("peak_start_reading"));
            double peakEndReading = MathUtil.ObjToDouble(peak.get("peak_end_reading"));
            peakUsage += peakEndReading - peakStartReading;
        }
        return Map.of("total_usage", totalUsage,"peak_usage", peakUsage);
    }

    private Map<String, Object> getPopulatePeriodList(
                                    int dominateIntervalMinutes,
                                    List<Map<String, Object>> meterReading,
                                    LocalDateTime monthBegin,
                                    LocalDateTime firstReadTime, LocalDateTime lastReadTime,
                                    int daysInMonth,
                                    int peakFromHour, int peakFromMinute, int peakToHour, int peakToMinute){

        LocalDateTime current = monthBegin;
        double maxDemand = 0;

        List<Map<String, Object>> periodList = new ArrayList<>();
        List<Map<String, Object>> peakList = new ArrayList<>();

        int fullIndex = 0;
        for(int i = 0; i < daysInMonth + 1; i++){
            int year = monthBegin.getYear();
            int month = monthBegin.getMonthValue();
            int day = i + 1;
            if(i == daysInMonth){
                day = 1;
                month++;
                if(month > 12){
                    month = 1;
                    year++;
                }
            }
            LocalDateTime peakStart = LocalDateTime.of(year, month, day, peakFromHour, peakFromMinute);
            LocalDateTime peakEnd = LocalDateTime.of(year, month, day, peakToHour, peakToMinute);
            double peakStartReading = 0;
            LocalDateTime peakStartDt = null;
            double peakEndReading = 0;
            LocalDateTime peakEndDt = null;

            int periodIndex = 0;
            for(int j = 1; j <= 24 * 60 / dominateIntervalMinutes; j++){
                if(i == daysInMonth && j > 1){
                    break;
                }
                fullIndex++;
                periodIndex++;
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                row.put("fullIndex", fullIndex);

                for(Map<String, Object> reading : meterReading){
                    LocalDateTime dt = DateTimeUtil.getLocalDateTime((String) reading.get("dt"));
                    String dtStr = DateTimeUtil.getLocalDateTimeStr(dt);
                    LocalDateTime left = current.minusMinutes(3);
                    LocalDateTime right = current.plusMinutes(3);
                    if(dt.isAfter(left) && dt.isBefore(right)){
                        // add to period list
                        row.put("dt", dt);
                        row.put("dtStr", dtStr);

                        row.put("periodIndex", periodIndex);

                        Object aImpObj = reading.get("a_imp");
                        Double aImp = null;
                        if(aImpObj != null) {
                            aImp = MathUtil.setDecimalPlaces(MathUtil.ObjToDouble(aImpObj), 2, RoundingMode.HALF_UP);
                        }
                        row.put("a_imp", aImp==null ? reading.get("a_imp") : aImp);

                        Double prevAImp = null;
                        if(!periodList.isEmpty()){
                            Object prevAImpObj = periodList.getLast().get("a_imp");
                            if(prevAImpObj != null) {
                                prevAImp = MathUtil.ObjToDouble(prevAImpObj);
                            }
                        }
                        if(prevAImp != null){
                            double diff = MathUtil.ObjToDouble(row.get("a_imp")) - prevAImp;
                            row.put("diff", diff);
                        }else{
                            row.put("diff", "-");
                        }

                        row.put("ref", reading.get("ref")==null? " " : reading.get("ref"));

                        double maxD =  Double.parseDouble((String)reading.get("c_md_sb_a_imp"));
                        if(maxD > maxDemand){
                            maxDemand = maxD;
                        }

                        if(!periodList.isEmpty()) {
                            if (peakStartReading == 0) {
                                Map<String, Object> periodItem = periodList.getLast();
                                if(periodItem.containsKey("dt")) {
                                    LocalDateTime lastDt = (LocalDateTime) periodItem.get("dt");
                                    LocalDateTime currentDt = (LocalDateTime) row.get("dt");
                                    //last Dt is before peak start time and current Dt is after peak start time
                                    if (lastDt.isBefore(peakStart) && currentDt.isAfter(peakStart)) {
                                        peakStartReading = MathUtil.ObjToDouble(row.get("a_imp"));
                                        peakStartDt = (LocalDateTime) row.get("dt");
                                    }
                                }
                            }
                            if (peakEndReading == 0) {
                                Map<String, Object> periodItem = periodList.getLast();
                                if(periodItem.containsKey("dt")) {
                                    LocalDateTime lastDt = (LocalDateTime) periodItem.get("dt");
                                    LocalDateTime currentDt = (LocalDateTime) row.get("dt");
                                    //last Dt is before peak end time and current Dt is after peak end time
                                    if (lastDt.isBefore(peakEnd) && currentDt.isAfter(peakEnd)) {
                                        peakEndReading = MathUtil.ObjToDouble(row.get("a_imp"));
                                        peakEndDt = (LocalDateTime) periodItem.get("dt");
                                    }
                                }
                            }
                        }

                        break;
                    }
                }

                periodList.add(row);
                current = current.plusMinutes(dominateIntervalMinutes);
            }
            if(peakStartReading != 0 && peakEndReading != 0) {
                peakList.add(Map.of(
                        "day_of_the_month", i+1,
                        "peak_start_reading", peakStartReading,
                        "peak_start_dt", peakStartDt,
                        "peak_end_reading", peakEndReading,
                        "peak_end_dt", peakEndDt
                ));
            }
        }
        //drop "dt" from periodList
        periodList.forEach(period -> period.remove("dt"));
        //add "-" in case "dtStr" is null, add "-" in case "a_imp" is null
        periodList.forEach(period -> {
            period.putIfAbsent("dtStr", "-");
            period.putIfAbsent("a_imp", "-");
        });

        return Map.of("period_list", periodList, "peak_list", peakList, "max_demand", maxDemand);
    }
}
