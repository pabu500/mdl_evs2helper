package org.pabuff.evs2helper;

import org.pabuff.dto.*;
import org.pabuff.oqghelper.QueryHelper;
import org.pabuff.utils.DateTimeUtil;
import org.pabuff.utils.MathUtil;
import org.pabuff.utils.XtStat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Service
public class DataNormalizer {
    private final Logger logger = Logger.getLogger(DataNormalizer.class.getName());

    @Autowired
    private QueryHelper queryHelper;

    public List<Map<String, Object>> normalizedTransactionList(List<Map<String, Object>> transactions, boolean queryOpsId){
        List<Map<String, Object>> normalizedList = new ArrayList<>();

        for(Map<String, Object> transaction : transactions){
            //get all map entry and copy to normalizedList
            Map<String, Object> normalizedTransaction = new HashMap<>();
            boolean isVirtual = false;
            for(Map.Entry<String, Object> entry : transaction.entrySet()){
                switch (entry.getKey()) {
                    case "payment_mode" -> {
                        Object paymentModeId = transaction.get("payment_mode");

                        String paymentModeStr = TransactionDef.paymentMode.get(MathUtil.ObjToInteger(paymentModeId));
                        if (paymentModeStr == null) {
                            paymentModeStr = paymentModeId.toString();
                        }
                        entry.setValue(paymentModeStr);
                        if(paymentModeStr.equalsIgnoreCase("Virtual")) {
                            isVirtual = true;
                        }
                    }
                    case "payment_channel" -> {
                        Object paymentChannelId = transaction.get("payment_channel");
                        String paymentChannelStr = TransactionDef.paymentChannel.get(MathUtil.ObjToInteger(paymentChannelId));
                        if (paymentChannelStr == null) {
                            paymentChannelStr = paymentChannelId.toString();
                        }
                        entry.setValue(paymentChannelStr);
                    }
                    case "transaction_status" -> {
                        Object transactionStatusId = transaction.get("transaction_status");
                        String transactionStatusStr = TransactionDef.transactionStatus.get(MathUtil.ObjToInteger(transactionStatusId));
                        if (transactionStatusStr == null) {
                            transactionStatusStr = transactionStatusId.toString();
                        }
                        entry.setValue(transactionStatusStr);
                    }
                }
                normalizedTransaction.put(entry.getKey(), entry.getValue());
            }

            if(isVirtual&&queryOpsId){
                String transactionId = (String) transaction.get("transaction_id");
                Map<String, Object> ops = queryHelper.getOpsLogItem("ref_id", transactionId);
                if(ops!=null) {
                    normalizedTransaction.put("ops_name", ops.get("username"));
                }
            }

            normalizedList.add(normalizedTransaction);
        }
        return normalizedList;
    }

    public static List<Map<String, Object>> normalizedTariffList(List<Map<String, Object>> tariffs, String meterDisplayname){
        List<Map<String, Object>> normalizedTariff = new ArrayList<>();

        int i = 0;
        for(Map<String, Object> tariff : tariffs){
            if (i == tariffs.size()-1) {
                continue;
            }
            double creditBalance = MathUtil.ObjToDouble(tariff.get("credit_balance"));
            double creditBalanceDiff = creditBalance - MathUtil.ObjToDouble(tariffs.get(i + 1).get("credit_balance"));

            tariff.put("debit_credit", creditBalanceDiff);
            tariff.put("meter_displayname", meterDisplayname);
            normalizedTariff.add(tariff);

            i++;
        }
        return normalizedTariff;
    }

    public static List<Map<String, Object>> normalizedMeterTariffList(List<Map<String, Object>> tariffs, String meterDisplayname){
        List<Map<String, Object>> normalizedTariff = new ArrayList<>();

        int i = 0;
        for(Map<String, Object> tariff : tariffs){
            //format decimal
            Double kwhDiff = MathUtil.ObjToDouble(tariff.get("kwh_diff"));
            String kwhDiffFormatted = kwhDiff==null? null: String.format("%.3f", kwhDiff);
            tariff.put("kwh_diff", kwhDiffFormatted);
            Double refKwh = MathUtil.ObjToDouble(tariff.get("ref_kwh_total"));
            String refKwhFormatted = refKwh==null? null: String.format("%.3f", refKwh);
            tariff.put("ref_kwh_total", refKwhFormatted);
            Double tariffPrice = MathUtil.ObjToDouble(tariff.get("tariff_price"));
            String tariffPriceFormatted = tariffPrice==null? null: String.format("%.2f", tariffPrice);
            tariff.put("tariff_price", tariffPriceFormatted);
            Double credit_amt = MathUtil.ObjToDouble(tariff.get("credit_amt"));
            String creditAmtFormatted = credit_amt==null? null: String.format("%.2f", credit_amt);
            tariff.put("credit_amt", creditAmtFormatted);
            Double debit_amt = MathUtil.ObjToDouble(tariff.get("debit_amt"));
            String debitAmtFormatted = debit_amt==null?null: String.format("%.2f", debit_amt);
            tariff.put("debit_amt", debitAmtFormatted);
            Double refBal = MathUtil.ObjToDouble(tariff.get("ref_bal"));
            String refBalFormatted = String.format("%.2f", refBal);
            tariff.put("ref_bal", refBalFormatted);

            normalizedTariff.add(tariff);
            i++;
        }
        return normalizedTariff;
    }

    public IotHistoryDto normalizeIotHistory(String deviceID, List<Map<String, Object>> resp,
                                             String normalizationTarget,
                                             String normalizeField,
                                             boolean allowConsolidation,
                                             boolean genMeta, boolean getStatOnly) {

        boolean test = false;
        if(normalizationTarget.equals(HistoryType.meter_reading.toString())) {
            return normalizeMeterReading(deviceID, resp);
        }else if (normalizationTarget.equals(HistoryType.meter_reading_daily.toString())) {
            return normalizeMeterReadingDaily(resp);
        }else if (normalizationTarget.equals(HistoryType.meter_reading_3p.toString())) {
            return normalizeMultiPartReading2(HistoryType.meter_reading_3p,
                    "", normalizeField, deviceID, "dt", resp, null,
                    Map.of("clearRepeatedReadingOnly", true,
                           "allowConsolidation", true,
                           "genMeta", true,
                           "getStatOnly", true
                    )
            );
        }else if (normalizationTarget.equals(HistoryType.meter_reading_3p_daily.toString())) {
            return normalizeMeterReading3pDaily(resp);
        }else if (normalizationTarget.equals(HistoryType.sensor_reading.toString())) {
            return normalizeMultiPartReading2(HistoryType.sensor_reading,
                    "", normalizeField, deviceID, "dt", resp, null,
                    Map.of("clearRepeatedReadingOnly", true,
                           "allowConsolidation", true,
                           "genMeta", true,
                           "getStatOnly", true
                    ));
        }
        return null;
    }

    public IotHistoryDto normalizeIotHistory2(String normalization, String deviceID, List<Map<String, Object>> resp,
                                              String normalizationTarget, String normalizeField,
                                              Double errorThreshold,
                                              Map<String, Object> config) {

        boolean test = false;
        if(normalizationTarget.equals(HistoryType.meter_reading.toString())) {
            return normalizeMultiPartReading2(HistoryType.meter_reading,
                    normalization, normalizeField, deviceID,
                    "kwh_timestamp", resp, errorThreshold, config);
        }else if (normalizationTarget.equals(HistoryType.meter_reading_daily.toString())) {
            return normalizeMeterReadingDaily(resp);
        }else if (normalizationTarget.equals(HistoryType.meter_reading_3p.toString())) {
            return normalizeMultiPartReading2(HistoryType.meter_reading_3p,
                    normalization, normalizeField, deviceID,
                    "dt", resp, errorThreshold, config);
        }else if (normalizationTarget.equals(HistoryType.meter_reading_3p_daily.toString())) {
            return normalizeMeterReading3pDaily(resp);
        }else if (normalizationTarget.equals(HistoryType.sensor_reading.toString())) {
            return normalizeMultiPartReading2(HistoryType.sensor_reading,
                    normalization, normalizeField, deviceID,
                    "dt", resp, errorThreshold, config);
        }else if(normalizationTarget.equals(HistoryType.meter_reading_iwow.toString())){
            return normalizeMultiPartReading2(HistoryType.meter_reading_iwow,
                    normalization, normalizeField, deviceID,
                    "dt", resp, errorThreshold, config);
        }else if(normalizationTarget.equals(HistoryType.fleet_health.toString())){
            return normalizeMultiPartReading2(HistoryType.fleet_health,
                    normalization, normalizeField, deviceID,
                    "poll_timestamp", resp, errorThreshold, config);
        }
        return null;
    }

    private static IotHistoryDto normalizeMeterReadingDaily(List<Map<String, Object>> srcKwhReading) {

        List<IotHistoryRowDto> iotHistoryNormalized = new ArrayList<>();
        List<IotHistoryRowDto> iotHistory = new ArrayList<>();
        List<Double> intervals = new ArrayList<>();
        List<Double> diffs = new ArrayList<>();
        long dominantIntervalHours = 0;
        double maxIntervalHours = 0;

        for (int i = 0; i < srcKwhReading.size(); i++) {
            Map<String, Object> row = srcKwhReading.get(i);
            if (i == srcKwhReading.size() - 1) {
                continue;
            }

            double reading_total = MathUtil.ObjToDouble(row.get("kwh_total"));
            double kwhDiff = reading_total - MathUtil.ObjToDouble(srcKwhReading.get(i + 1).get("kwh_total"));
            diffs.add(kwhDiff);

            LocalDateTime t1 = DateTimeUtil.getLocalDateTime((String) row.get("kwh_timestamp"));
            LocalDateTime t2 = DateTimeUtil.getLocalDateTime((String) srcKwhReading.get(i + 1).get("kwh_timestamp"));
            long interval = Duration.between(t2, t1).toMillis();
            double intervalHours = (double) interval / 1000 / 60 / 60;
            intervals.add(intervalHours);

            IotHistoryRowDto rowDto = IotHistoryRowDto.builder()
                    .readingTimestamp(t1)
                    .readingTotal(reading_total)
                    .readingDiff(kwhDiff)
                    .readingInterval(interval)
                    .isEstimated(false)
                    .build();
            iotHistory.add(rowDto);

        }
        //find the dominant interval
        dominantIntervalHours = MathUtil.findDominantLong(intervals);
        if (dominantIntervalHours == 0) {
            dominantIntervalHours = 1;
        }
        maxIntervalHours = MathUtil.findMax(intervals);
        //insert estimated rows
        if (maxIntervalHours > 2 * dominantIntervalHours) {
            insertEstimate(iotHistory, dominantIntervalHours*60);
        }
        iotHistoryNormalized = iotHistory;

        //find duration of the list in resp
        LocalDateTime end = DateTimeUtil.getLocalDateTime((String) srcKwhReading.get(0).get("kwh_timestamp"));
        LocalDateTime start = DateTimeUtil.getLocalDateTime((String) srcKwhReading.get(srcKwhReading.size() - 1).get("kwh_timestamp"));
        long duration = Duration.between(start, end).toMillis();
        double averageReading = MathUtil.findAverage(diffs);
        double total = MathUtil.findTotal(diffs);

        IotHistoryMetaDto meta = IotHistoryMetaDto.builder()
                .dominantInterval(dominantIntervalHours * DateTimeUtil.oneHour)
                .duration(duration)
                .avgVal(averageReading)
                .total(total)
                .maxVal(MathUtil.findMax(iotHistory.stream().map(IotHistoryRowDto::getReadingDiff).collect(Collectors.toList())))
                .build();

        return IotHistoryDto.builder().history(iotHistoryNormalized).meta(meta).build();
    }

    private IotHistoryDto normalizeMeterReading(String deviceID, List<Map<String, Object>> resp) {

        //        List<Map<String, Object>> respNormalized = new ArrayList<>();
        List<IotHistoryRowDto> iotHistoryNormalized = new ArrayList<>();

//        List<Map<String, ImmutablePair<Double, Double>>> diff = new ArrayList<>();
        List<IotHistoryRowDto> iotHistory = new ArrayList<>();
        List<Double> intervals = new ArrayList<>();
        List<Double> diffs = new ArrayList<>();
        long dominantInterval = 0;
        double maxInterval = 0;
        Map<String, Long> repeatedReading = new HashMap<>();
        //convert to IotHistoryRowDto
        for (int i = 0; i < resp.size(); i++) {
            Map<String, Object> row = resp.get(i);
//                Map<String, ImmutablePair<Double, Double>> rowDiff = new HashMap<>();
            if (i == resp.size()-1) {
//                    rowDiff.put("kwh_diff", row.get("kwh_total"));
            } else {
                double reading_total = MathUtil.ObjToDouble(row.get("kwh_total"));
                double kwhDiff = reading_total - MathUtil.ObjToDouble(resp.get(i + 1).get("kwh_total"));
                diffs.add(kwhDiff);

                String t1str = (String) row.get("kwh_timestamp");
                String t2str = (String) resp.get(i + 1).get("kwh_timestamp");
                LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1str);
                LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2str);

                //check if the two timestamps are identical (repeated reading)
                if(t1.equals(t2)) {
                    if(repeatedReading.containsKey(t1str)) {
                        repeatedReading.put(t1str, repeatedReading.get(t1str) + 1);
                    }else {
                        repeatedReading.put(t1str, 1L);
                    }
                    continue;
                }

                long interval = Duration.between(t2, t1).toMillis();
                double intervalMin = (double) interval / 1000 / 60;
                intervals.add(intervalMin);

//                    ImmutablePair<Double, Double> diffPair = new ImmutablePair<>(intervalMin, kwhDiff);
//                    rowDiff.put((String) row.get("kwh_timestamp"), diffPair);
                IotHistoryRowDto rowDto = IotHistoryRowDto.builder()
                        .readingTimestamp(t1)
                        .readingTotal(reading_total)
                        .readingDiff(kwhDiff)
                        .readingInterval(interval)
                        .isEstimated(false)
                        .build();
                iotHistory.add(rowDto);
            }
        }

        //post to kiv if there is repeated reading
        if(!repeatedReading.isEmpty()) {
            //post the first repeated timestamp
            for (String timestamp : repeatedReading.keySet()) {
                String localNow = DateTimeUtil.getSgNowStr();
                LinkedHashMap<String, Object> kivRefVals = new LinkedHashMap<>();
                kivRefVals.put("repeated_reading_count", repeatedReading.get(timestamp));
                kivRefVals.put("val2", 0D);
                kivRefVals.put("val3", 0D);
                kivRefVals.put("timestamp", timestamp);
                queryHelper.postMeterKiv4(deviceID,
                        "repeated_reading",
                        localNow,
                        repeatedReading.size(),
                        kivRefVals,
                        "oresvc",
                        UUID.randomUUID().toString()
                );
                break;
            }
        }

        //find the dominant interval
        dominantInterval = MathUtil.findDominantLong(intervals);
        if(dominantInterval == 0) {
            dominantInterval = 1;
        }
        maxInterval = MathUtil.findMax(intervals);
        //insert estimated rows
        if(maxInterval > 2*dominantInterval) {
            insertEstimate(iotHistory, dominantInterval);
        }
        iotHistoryNormalized = iotHistory;

        //find duration of the list in resp
        LocalDateTime end = DateTimeUtil.getLocalDateTime((String) resp.get(0).get("kwh_timestamp"));
        LocalDateTime start = DateTimeUtil.getLocalDateTime((String) resp.get(resp.size()-1).get("kwh_timestamp"));
        long duration = Duration.between(start, end).toMillis();
        double averageReading = MathUtil.findAverage(diffs);
        double total = MathUtil.findTotal(diffs);
        long totalPositive = MathUtil.findPositiveCount(diffs);

        IotHistoryMetaDto meta = IotHistoryMetaDto.builder()
                .dominantInterval(dominantInterval*DateTimeUtil.oneMinute)
                .duration(duration)
                .avgVal(averageReading)
                .total(total)
                .positiveCount(totalPositive)
                .maxVal(MathUtil.findMax(iotHistory.stream().map(IotHistoryRowDto::getReadingDiff).collect(Collectors.toList())))
                .build();

        return IotHistoryDto.builder().history(iotHistoryNormalized).meta(meta).build();
    }

    private static IotHistoryDto normalizeMeterReading3pDaily(List<Map<String, Object>> srcKwhReading) {

        List<IotHistoryRowDto> iotHistoryNormalized = new ArrayList<>();
        List<IotHistoryRowDto> iotHistory = new ArrayList<>();
        List<Double> intervals = new ArrayList<>();
        List<Double> diffs = new ArrayList<>();
        long dominantIntervalHours = 0;
        double maxIntervalHours = 0;

        for (int i = 0; i < srcKwhReading.size(); i++) {
            Map<String, Object> row = srcKwhReading.get(i);
            if (i == srcKwhReading.size() - 1) {
                continue;
            }

            double reading_total = MathUtil.ObjToDouble(row.get("kwh_total"));
            double kwhDiff = reading_total - MathUtil.ObjToDouble(srcKwhReading.get(i + 1).get("kwh_total"));
            diffs.add(kwhDiff);

            LocalDateTime t1 = DateTimeUtil.getLocalDateTime((String) row.get("kwh_timestamp"));
            LocalDateTime t2 = DateTimeUtil.getLocalDateTime((String) srcKwhReading.get(i + 1).get("kwh_timestamp"));
            long interval = Duration.between(t2, t1).toMillis();
            double intervalHours = (double) interval / 1000 / 60 / 60;
            intervals.add(intervalHours);

            IotHistoryRowDto rowDto = IotHistoryRowDto.builder()
                    .readingTimestamp(t1)
                    .readingTotal(reading_total)
                    .readingDiff(kwhDiff)
                    .readingInterval(interval)
                    .isEstimated(false)
                    .build();
            iotHistory.add(rowDto);

        }
        //find the dominant interval
        dominantIntervalHours = MathUtil.findDominantLong(intervals);
        if (dominantIntervalHours == 0) {
            dominantIntervalHours = 1;
        }
        maxIntervalHours = MathUtil.findMax(intervals);
        //insert estimated rows
        if (maxIntervalHours > 2 * dominantIntervalHours) {
            insertEstimate(iotHistory, dominantIntervalHours*60);
        }
        iotHistoryNormalized = iotHistory;

        //find duration of the list in resp
        LocalDateTime end = DateTimeUtil.getLocalDateTime((String) srcKwhReading.get(0).get("kwh_timestamp"));
        LocalDateTime start = DateTimeUtil.getLocalDateTime((String) srcKwhReading.get(srcKwhReading.size() - 1).get("kwh_timestamp"));
        long duration = Duration.between(start, end).toMillis();
        double averageReading = MathUtil.findAverage(diffs);
        double total = MathUtil.findTotal(diffs);

        IotHistoryMetaDto meta = IotHistoryMetaDto.builder()
                .dominantInterval(dominantIntervalHours * DateTimeUtil.oneHour)
                .duration(duration)
                .avgVal(averageReading)
                .total(total)
                .maxVal(MathUtil.findMax(iotHistory.stream().map(IotHistoryRowDto::getReadingDiff).collect(Collectors.toList())))
                .build();

        return IotHistoryDto.builder().history(iotHistoryNormalized).meta(meta).build();
    }

    public IotHistoryDto normalizeMultiPartReading2(HistoryType historyTypeEnum,
                                                    String normalization,
                                                    String normalizeFields,
                                                    String deviceId,
                                                    String timeField,
                                                    List<Map<String, Object>> rawDataRows,
                                                    Double errorThreshold,
                                                    Map<String, Object> config
                                                    ) {

        String[] parts = normalizeFields.trim().replace(" ", "").split(",");

        boolean rawDataCheck = config.get("rawDataCheck") != null && (Boolean) config.get("rawDataCheck");
        boolean clearRepeatedReadingsOnly = config.get("clearRepeatedReadingsOnly") != null && (Boolean) config.get("clearRepeatedReadingsOnly");
        boolean detectRestartEvent = config.get("detectRestartEvent") != null && (Boolean) config.get("detectRestartEvent");
        boolean forceAlignTimeRange = config.get("forceAlignTimeRange") != null && (Boolean) config.get("forceAlignTimeRange");
        boolean intervalClean = config.get("intervalClean") != null && (Boolean) config.get("intervalClean");

        Map<String, Object> intervalCleanResult = new HashMap<>();
        if(intervalClean) {
            intervalCleanResult = cleanInterval(rawDataRows, timeField, clearRepeatedReadingsOnly, detectRestartEvent);
        }else{
            intervalCleanResult = Map.of(
                    "cleaned_data", rawDataRows,
                    "dominant_interval", 1L,
                    "max_interval", 1D,
                    "outlier_count", 0L
            );
        }
        List<Map<String, Object>> intervalCleanedDataRows = (List<Map<String, Object>>) intervalCleanResult.get("cleaned_data");
        long dominantIntervalMinute = (long) intervalCleanResult.get("dominant_interval");
        double maxInterval = (double) intervalCleanResult.get("max_interval");
        long outliers = (long) intervalCleanResult.get("outlier_count");
        long cleanCount = intervalCleanedDataRows.size();

        if(rawDataCheck){
            return doRawDataCheck(
                intervalCleanedDataRows,
                timeField, parts,
                dominantIntervalMinute, errorThreshold
            );
        }

        List<IotHistoryRowDto2> iotHistory = new ArrayList<>();
        // convert to IotHistoryRowDto
        for (int i = 0; i < intervalCleanedDataRows.size(); i++) {
            if (i == intervalCleanedDataRows.size() - 1) continue;

            Map<String, Object> row = intervalCleanedDataRows.get(i);
            Map<String, Object> prevRow = intervalCleanedDataRows.get(i+1);

            //reading time
            String t1str = (String) row.get(timeField);
            String t2str = (String) prevRow.get(timeField);
            LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1str);
            LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2str);

            long interval = Duration.between(t2, t1).toMillis();
            double intervalSecond = (double) interval / 1000;

            // reading data
            Map<String, Map<String, Object>> readings = new HashMap<>();
            int readingsContainError = 0;
            for (String part : parts) {
                if (row.get(part) == null || prevRow.get(part) == null){
                    continue;
                }
                double readingTotal = MathUtil.ObjToDouble(row.get(part));
                double readingTotal2 = MathUtil.ObjToDouble(prevRow.get(part));
                double readingDiff = readingTotal - readingTotal2;
                Map<String, Object> readingPart = new HashMap<>();
                readingPart.put("rt", readingTotal);
//                readingPart.put("rd", readingDiff);
                readingPart.put("rd", MathUtil.round(readingDiff, 5));

                //check error threshold
                if(errorThreshold!=null && Math.abs(readingDiff) > errorThreshold) {
                    readingPart.put("rd_is_err", 1);
                    readingsContainError = 1;
                }
                //check restart event
                if(detectRestartEvent) {
                    if(row.containsKey("restart")) {
                        readingPart.put("rd_is_restart", 1);
                    }
                }
                readings.put(part, readingPart);
            }
            IotHistoryRowDto2 rowDto = IotHistoryRowDto2.builder()
                    .dt(t1)
                    .readings(readings)
                    .riS(MathUtil.round(intervalSecond, 3))
                    .isEst(0)
                    .isOt(readingsContainError)
                    .build();
            iotHistory.add(rowDto);
        }

        //post to kiv if there is repeated reading
        Map<String, Long> repeatedReading = new HashMap<>();
//        if (postKiv) {
        if(false) {
            if (!repeatedReading.isEmpty()) {
                //post the first repeated timestamp
                for (String timestamp : repeatedReading.keySet()) {
                    String localNow = DateTimeUtil.getSgNowStr();
                    LinkedHashMap<String, Object> kivRefVals = new LinkedHashMap<>();
                    kivRefVals.put("repeated_reading_count", repeatedReading.get(timestamp));
                    kivRefVals.put("val2", 0D);
                    kivRefVals.put("val3", 0D);
                    kivRefVals.put("timestamp", timestamp);
                    try {
                        queryHelper.postMeterKiv4(deviceId,
                                "repeated_reading",
                                localNow,
                                repeatedReading.size(),
                                kivRefVals,
                                "oresvc",
                                UUID.randomUUID().toString()
                        );
                    } catch (Exception e) {
                        logger.info("error posting repeated reading to kiv: " + e.getMessage());
                    }
                }
            }
        }

        // offset negative reading
        long totalNegCount = 0;
        if (normalization.contains("none")) {
        } else {
            totalNegCount = normalizeNegativeReading(iotHistory);
            logger.info("offset " + totalNegCount + " negative rows");
        }

        // insert estimated rows
        long totalInsertCount = 0;
        boolean insertZero = false;
        if (normalization.contains("none")) {
            insertZero = true;
        }
        if (maxInterval > 1.95 * dominantIntervalMinute) {
            totalInsertCount = insertMultiPartEstimate2(iotHistory, dominantIntervalMinute, insertZero);
            logger.info("inserted " + totalInsertCount + " estimated rows");
        }

        // check error threshold
        if(errorThreshold!=null) {
            for (IotHistoryRowDto2 row : iotHistory) {
                Integer isOt = row.getIsOt();
                if(isOt == null) continue;
                if (isOt == 0) continue;
                Map<String, Map<String, Object>> readings = row.getReadings();
                for (String part : parts) {
                    Map<String, Object> reading = readings.get(part);
                    double readingDiff = MathUtil.ObjToDouble(reading.get("rd"));
                    if (Math.abs(readingDiff) > errorThreshold) {
                        reading.put("rd_is_err", 1);
                    }
                }
            }
        }

        List<IotHistoryRowDto2> iotHistoryNormalized = iotHistory;

        //meta data
        Map<String, List<Double>> totalsMap = new HashMap<>();
        Map<String, List<Double>> diffsMap = new HashMap<>();
        for(String part : parts) {
            if(iotHistoryNormalized.stream().map(IotHistoryRowDto2::getReadings).map(m -> m.get(part)).anyMatch(Objects::isNull)){
                continue;
            }
            List<Double> partTotals = iotHistoryNormalized.stream().map(IotHistoryRowDto2::getReadings).map(m -> m.get(part)).map(m -> MathUtil.ObjToDouble(m.get("rt"))).collect(Collectors.toList());
            totalsMap.put(part, partTotals);
            List<Double> partDiffs = iotHistoryNormalized.stream().map(IotHistoryRowDto2::getReadings).map(m -> m.get(part)).map(m -> MathUtil.ObjToDouble(m.get("rd"))).collect(Collectors.toList());
            diffsMap.put(part, partDiffs);
        }

        boolean genMeta = config.get("genMeta") == null || (Boolean) config.get("genMeta");
        if(!genMeta) {
            return IotHistoryDto.builder().history2(iotHistoryNormalized).build();
        }

        Map<String, IotHistoryMetaDto> metaMap = new HashMap<>();
        LocalDateTime end = iotHistoryNormalized.getFirst().getDt();
        LocalDateTime start = iotHistoryNormalized.getLast().getDt();
        for (String part : parts) {
            if(totalsMap.get(part)==null || diffsMap.get(part)==null) {
                continue;
            }
            long duration = Duration.between(start, end).toMillis();
            List<Double> partReading = totalsMap.get(part);
            XtStat stat = MathUtil.findStat(partReading);
            double averageReading = stat.getAvg();
            double medianVal = stat.getMedian();
            double maxVal = stat.getMax();
            double minVal = stat.getMin();
            double minValNonZero = stat.getMinNonZero();
            double total = stat.getTotal();
            long totalPositiveCount = stat.getPositiveCount();
            long totalNegativeCount = totalNegCount; //stat.getNegativeCount();
            long totalCount = stat.getTotalCount();
            IotHistoryMetaDto meta = IotHistoryMetaDto.builder()
                    .dominantInterval(dominantIntervalMinute * DateTimeUtil.oneMinute)
                    .duration(duration)
                    .avgVal(averageReading)
                    .medianVal(medianVal)
                    .total(total)
                    .positiveCount(totalPositiveCount)
                    .maxVal(maxVal)
                    .minVal(minVal)
                    .minValNonZero(minValNonZero)
//                    .totalCount(cleanCount)
                    .rawDataCount((long)rawDataRows.size())
                    .build();
            metaMap.put(part, meta);

            List<Double> partDiffs = diffsMap.get(part);
            double averageReadingDiffs = MathUtil.findAverage(partDiffs);
            double medianValDiffs = MathUtil.findMedian(partDiffs);
            double maxValDiffs = MathUtil.findMax(partDiffs);
            double minValDiffs = MathUtil.findMin(partDiffs);
            double minValNonZeroDiffs = MathUtil.findMinNonZero(partDiffs);
            double totalDiffs = MathUtil.findTotal(partDiffs);
            long totalPositiveDiffs = MathUtil.findPositiveCount(partDiffs);

            IotHistoryMetaDto metaDiff = IotHistoryMetaDto.builder()
                    .dominantInterval(dominantIntervalMinute * DateTimeUtil.oneMinute)
                    .duration(duration)
                    .avgVal(averageReadingDiffs)
                    .medianVal(medianValDiffs)
                    .total(totalDiffs)
                    .positiveCount(totalPositiveDiffs)
                    .maxVal(maxValDiffs)
                    .minVal(minValDiffs)
                    .minValNonZero(minValNonZeroDiffs)
//                    .totalCount(cleanCount)
                    .negativeDiffCount(totalNegativeCount)
                    .intervalOutlierCount(outliers)
                    .rawDataCount((long)rawDataRows.size())
                    .build();
            metaMap.put(part+"_diff", metaDiff);
        }

        boolean getStatOnly = config.get("getStatOnly") != null && (Boolean) config.get("getStatOnly");
        if(getStatOnly){
            return IotHistoryDto.builder().metas(metaMap).build();
        }

        boolean allowConsolidation = (Boolean) config.get("allowConsolidation");
        if(allowConsolidation) {
            String targetInterval = "";
            if (historyTypeEnum == HistoryType.meter_reading) {
                if (iotHistoryNormalized.size() > 500) {
                    targetInterval = "hourly";
                }
            } else if (historyTypeEnum == HistoryType.meter_reading_3p) {
                if (iotHistoryNormalized.size() > 2160) {
                    targetInterval = "daily";
                }
            } else if (historyTypeEnum == HistoryType.meter_reading_iwow) {
                if (iotHistoryNormalized.size() > 340) {
                    targetInterval = "hourly";
                }
            }else if (historyTypeEnum == HistoryType.sensor_reading) {
                if (iotHistoryNormalized.size() > 500) {
                    targetInterval = "hourly";
                }
            }

            if (!targetInterval.isEmpty()) {
                Map<String, IotHistoryDto> result =
                        consolidateMultiPartReadingDto(iotHistoryNormalized, dominantIntervalMinute, targetInterval, 1);
                return result.get("consolidated_history");
            }
        }

        if(forceAlignTimeRange){
            LocalDateTime targetStartDateTime = DateTimeUtil.getLocalDateTime((String) config.get("startDatetime"));
            LocalDateTime targetEndDateTime = DateTimeUtil.getLocalDateTime((String) config.get("endDatetime"));
            Map<String, Object> result =
                    alignTimeRange(iotHistoryNormalized, targetStartDateTime, targetEndDateTime, dominantIntervalMinute);
            iotHistoryNormalized = (List<IotHistoryRowDto2>) result.get("aligned_history");
        }

        return IotHistoryDto.builder().history2(iotHistoryNormalized).metas(metaMap).build();
    }
    public static Map<String, Object> alignTimeRange(
            List<IotHistoryRowDto2> historyDto2,
            LocalDateTime targetStartDateTime, LocalDateTime targetEndDateTime,
            long dominantIntervalMinute){
        if(dominantIntervalMinute<15){
            return Map.of("error", "dominant interval must be at least 15 minutes") ;
        }
        List<IotHistoryRowDto2> alignedHistory = new ArrayList<>();

        // insert zero if data starts after target start date or ends before target end date
        LocalDateTime dataStartDateTime = historyDto2.getLast().getDt();
        LocalDateTime dataEndDateTime = historyDto2.getFirst().getDt();

        if(targetEndDateTime.isAfter(dataEndDateTime.plusMinutes(dominantIntervalMinute))){
            LocalDateTime insertDatTime = dataEndDateTime.plusMinutes(dominantIntervalMinute);
            while(insertDatTime.isBefore(targetEndDateTime)){
                IotHistoryRowDto2 rowDto = IotHistoryRowDto2.builder()
                        .dt(insertDatTime)
                        .readings(new HashMap<>())
                        .riS(0D)
                        .isEst(0)
                        .isOt(0)
                        .isEmpty(1)
                        .build();
                alignedHistory.addFirst(rowDto);
                insertDatTime = insertDatTime.plusMinutes(dominantIntervalMinute);
            }
        }

        alignedHistory.addAll(historyDto2);

        if(targetStartDateTime.isBefore(dataStartDateTime.minusMinutes(dominantIntervalMinute))){
            LocalDateTime insertDatTime = dataStartDateTime.minusMinutes(dominantIntervalMinute);
            while(insertDatTime.isAfter(targetStartDateTime)){
                IotHistoryRowDto2 rowDto = IotHistoryRowDto2.builder()
                        .dt(insertDatTime)
                        .readings(new HashMap<>())
                        .riS(0D)
                        .isEst(0)
                        .isOt(0)
                        .isEmpty(1)
                        .build();
//                alignedHistory.add(0, rowDto);
                alignedHistory.add(rowDto);
                insertDatTime = insertDatTime.minusMinutes(dominantIntervalMinute);
            }
        }
        return Map.of("aligned_history", alignedHistory);
    }

    private static IotHistoryDto doRawDataCheck(
            List<Map<String, Object>> rawDataRows,
            String timeField, String[] parts,
            long dominantIntervalMinute,
            Double errorThreshold
    ) {
        String startTimeStr = (String) rawDataRows.getLast().get(timeField);
        String endTimeStr = (String) rawDataRows.getFirst().get(timeField);
        LocalDateTime startTime = DateTimeUtil.getLocalDateTime(startTimeStr);
        LocalDateTime endTime = DateTimeUtil.getLocalDateTime(endTimeStr);
        Duration duration = Duration.between(startTime, endTime);
        // -1 as first row is not counted due to no diff
        long expectedReadingCount = duration.toMinutes() / dominantIntervalMinute - 1;

        long repeatedReadingCount = 0;
        long missingReadingCount = 0;
        long negTotalCount = 0;
        long negDiffCount = 0;
        long overThresholdCount = 0;

        List<IotHistoryRowDto2> iotHistory = new ArrayList<>();
        for(int i = 0; i < rawDataRows.size(); i++) {
            if (i == rawDataRows.size() - 1) continue;

            Map<String, Object> row = rawDataRows.get(i);
            Map<String, Object> prevRow = rawDataRows.get(i+1);
            //reading time
            String t1str = (String) row.get(timeField);
            String t2str = (String) prevRow.get(timeField);
            LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1str);
            LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2str);

            long intervalSecond = Duration.between(t2, t1).toSeconds();

            int dtRepeated = 0;
            // repeat reading
            if(t1str.equals(t2str)) {
                row.put("dt_repeated", 1);
                dtRepeated = 1;
                repeatedReadingCount++;
            }
            // missing reading
            int dtMissing = 0;
            double intervalThreshold = dominantIntervalMinute * 60 * 1.5;
            if(intervalSecond > intervalThreshold) {
                row.put("dt_missing", 1);
                dtMissing = 1;
                missingReadingCount++;
            }

            Map<String, Map<String, Object>> readings = new HashMap<>();
            int readingsContainError = 0;
            for (String part : parts) {
                double readingTotal = MathUtil.ObjToDouble(row.get(part));
                double readingTotal2 = MathUtil.ObjToDouble(prevRow.get(part));
                double readingDiff = readingTotal - readingTotal2;
                Map<String, Object> readingPart = new HashMap<>();
                readingPart.put("rt", readingTotal);
                readingPart.put("rd", MathUtil.round(readingDiff, 5));
                readingPart.put("rd_neg", 0);

                // check neg
                if(readingDiff < 0){
                    row.put("rd_neg", 1);
                    readingPart.put("rd_neg", 1);
                    negDiffCount++;
                }
                if(readingTotal < 0){
                    row.put("rt_neg", 1);
                    readingPart.put("rt_neg", 1);
                    negTotalCount++;
                }
                //check error threshold
                if(errorThreshold != null && readingDiff > errorThreshold) {
                    row.put("rd_ot", 1);
                    readingPart.put("rd_ot", 1);
                    overThresholdCount++;
                }

                readings.put(part, readingPart);
            }

            IotHistoryRowDto2 rowDto = IotHistoryRowDto2.builder()
                    .dt(t1)
                    .readings(readings)
                    .dtRepeat(dtRepeated)
                    .dtMissing(dtMissing)
                    .riS(MathUtil.round(intervalSecond, 3))
                    .isEst(0)
                    .isOt(readingsContainError)
                    .build();
            iotHistory.add(rowDto);
        }

        Map<String, IotHistoryMetaDto> metaMap = new HashMap<>();

        IotHistoryMetaDto meta = IotHistoryMetaDto.builder()
            .firstValDt(startTime)
            .lastValDt(endTime)
            .duration(duration.toMillis())
            .dominantInterval(dominantIntervalMinute * DateTimeUtil.oneMinute)
            .rawDataCount((long) rawDataRows.size())
            .expectedReadingCount(expectedReadingCount)
            .repeatedReadingCount(repeatedReadingCount)
            .missingReadingCount(missingReadingCount)
            .negativeTotalCount(negTotalCount)
            .negativeDiffCount(negDiffCount)
            .overThresholdCount(overThresholdCount)
            .build();
        metaMap.put("raw_data_check", meta);

        return IotHistoryDto.builder().history2(iotHistory).metas(metaMap).build();
    }

    private static Map<String, Object> cleanInterval0(List<Map<String, Object>> rawDataRows,
                                                     String timeField,
                                                     boolean cleanRepeatedReadingOnly,
                                                     boolean detectRestartEvent) {

        List<Double> intervalsMinute = new ArrayList<>();
        // check/fix interval health
        // gen interval list
        for (int i = 0; i < rawDataRows.size(); i++) {
            Map<String, Object> row = rawDataRows.get(i);

            if (i == rawDataRows.size() - 1) continue;

            Map<String, Object> prevRow = rawDataRows.get(i+1);

            //reading time
            String t1str = (String) row.get(timeField);
            String t2str = (String) prevRow.get(timeField);
            LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1str);
            LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2str);

            long interval = Duration.between(t2, t1).toMillis();
            double intervalSecond = (double) interval / 1000;
            intervalsMinute.add(intervalSecond / 60);
        }
        // get interval stats from interval list
        Map<String, Double> intervalStat = MathUtil.findIntervalStat(intervalsMinute, 1.8);
        long dominantIntervalMinute = Math.round(intervalStat.get("dominant_interval"));
        double maxInterval = intervalStat.get("max_interval");
        long outliers = Math.round(intervalStat.get("outlier_count"));
        double minNonZeroInterval = intervalStat.get("min_non_zero_interval");

        // if dominant interval is 0, try snap to 15, 30, 60
        if (dominantIntervalMinute == 0) {
            //snap to 15, 30, 60
            if( Math.abs((maxInterval-15)/15)<0.05) {
                dominantIntervalMinute = 15;
            }else if( Math.abs((maxInterval-30)/30)<0.05) {
                dominantIntervalMinute = 30;
            }else if( Math.abs((maxInterval-60)/60)<0.05) {
                dominantIntervalMinute = 60;
            }else {
                dominantIntervalMinute = 1;
            }
        }

        List<Map<String, Object>> intervalCleanedDataRows = new ArrayList<>();
        if(cleanRepeatedReadingOnly) {
            for (int i = 0; i < rawDataRows.size(); i++) {
                Map<String, Object> row = rawDataRows.get(i);

                if (i == rawDataRows.size() - 1) continue;

                Map<String, Object> prevRow = rawDataRows.get(i+1);
                //reading time
                String t1str = (String) row.get(timeField);
                String t2str = (String) prevRow.get(timeField);
                if(!t1str.equals(t2str)) {
                    intervalCleanedDataRows.add(row);
                }
            }
            Map<String, Object> result = new HashMap<>();
            result.put("cleaned_data", intervalCleanedDataRows);
            result.put("dominant_interval", dominantIntervalMinute);
            result.put("outlier_count", outliers);
            result.put("max_interval", maxInterval);
            return result;
        }

        double minIntervalNonZeroSecond = minNonZeroInterval * 60;
        double thresholdSecond = 0.55 * dominantIntervalMinute * 60;

        if (minIntervalNonZeroSecond < thresholdSecond) {
            for (int i = 0; i < rawDataRows.size(); i++) {
                Map<String, Object> row = rawDataRows.get(i);
                if (i == rawDataRows.size() - 1) {
                    intervalCleanedDataRows.add(row);
                    continue;
                }

                //reading time
                String t1str = (String) row.get(timeField);
                String t2str = (String) rawDataRows.get(i + 1).get(timeField);
                LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1str);
                LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2str);

                long interval = Duration.between(t2, t1).toMillis();
                double intervalSecond = (double) interval / 1000;
                if (intervalSecond > thresholdSecond) {
                    intervalCleanedDataRows.add(row);
                }
            }
        }else {
            intervalCleanedDataRows = rawDataRows;
        }
        Map<String, Object> result = new HashMap<>();
        result.put("cleaned_data", intervalCleanedDataRows);
        result.put("dominant_interval", dominantIntervalMinute);
        result.put("outlier_count", outliers);
        result.put("max_interval", maxInterval);

        return result;
    }
    private Map<String, Object> cleanInterval(List<Map<String, Object>> rawDataRows,
                                              String timeField,
                                              boolean cleanRepeatedReadingOnly,
                                              boolean detectRestartEvent) {

        List<Double> intervalsMinute = new ArrayList<>();
        // check/fix interval health
        // gen interval list
        for (int i = 0; i < rawDataRows.size(); i++) {
            Map<String, Object> row = rawDataRows.get(i);

            if (i == rawDataRows.size() - 1) continue;

            Map<String, Object> prevRow = rawDataRows.get(i+1);

            //reading time
            String t1str = (String) row.get(timeField);
            String t2str = (String) prevRow.get(timeField);
            LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1str);
            LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2str);

            long interval = Duration.between(t2, t1).toMillis();
            double intervalSecond = (double) interval / 1000;
            intervalsMinute.add(intervalSecond / 60);
        }
        // get interval stats from interval list
        Map<String, Double> intervalStat = MathUtil.findIntervalStat(intervalsMinute, 1.8);
        long dominantIntervalMinute = Math.round(intervalStat.get("dominant_interval"));
        double maxInterval = intervalStat.get("max_interval");
        long outliers = Math.round(intervalStat.get("outlier_count"));
        double minNonZeroInterval = intervalStat.get("min_non_zero_interval");

        // if dominant interval is 0, try snap to 15, 30, 60
        if (dominantIntervalMinute == 0) {
            //snap to 15, 30, 60
            if( Math.abs((maxInterval-15)/15)<0.05) {
                dominantIntervalMinute = 15;
            }else if( Math.abs((maxInterval-30)/30)<0.05) {
                dominantIntervalMinute = 30;
            }else if( Math.abs((maxInterval-60)/60)<0.05) {
                dominantIntervalMinute = 60;
            }else {
                dominantIntervalMinute = 1;
            }
        }

        List<Map<String, Object>> intervalCleanedDataRows = new ArrayList<>();
        boolean hasRepeatedReading = false;

        for (int i = 0; i < rawDataRows.size(); i++) {
            Map<String, Object> row = rawDataRows.get(i);

            if (i == rawDataRows.size() - 1) continue;

            Map<String, Object> prevRow = rawDataRows.get(i+1);
            //reading time
            String t1str = (String) row.get(timeField);
            String t2str = (String) prevRow.get(timeField);
            if(!t1str.equals(t2str)) {
                intervalCleanedDataRows.add(row);
            }else{
                hasRepeatedReading = true;
            }
        }
        if(cleanRepeatedReadingOnly) {
            Map<String, Object> result = new HashMap<>();
            result.put("cleaned_data", intervalCleanedDataRows);
            result.put("dominant_interval", dominantIntervalMinute);
            result.put("outlier_count", outliers);
            result.put("max_interval", maxInterval);
            return result;
        }

        double cleanThreshold = 0.72;

        double minIntervalNonZeroSecond = minNonZeroInterval * 60;
        double thresholdSecond = cleanThreshold * dominantIntervalMinute * 60;

        List<Map<String, Object>> preCleaned = new ArrayList<>(intervalCleanedDataRows);

        intervalCleanedDataRows = new ArrayList<>();

        if (minIntervalNonZeroSecond > thresholdSecond) {
            intervalCleanedDataRows = preCleaned;
        }else{
            int restartIntervalThreadLower = 30;
            int restartIntervalThreadUpper = 80;
            int restartDetectionCounter = 0;
            boolean isCrossIntervalReadingAdded = false;

            for (int i = preCleaned.size()-1; i > 0; i--) {
                Map<String, Object> row = preCleaned.get(i);
//                if (i == rawDataRows.size() - 1) {
//                    intervalCleanedDataRows.add(row);
//                    continue;
//                }
                //reading time
                String t1Str = "";
                String t2Str = "";
                String tRefStr = "";
                if (intervalCleanedDataRows.isEmpty()) {
                    t1Str = (String) preCleaned.get(i - 1).get(timeField);
                    t2Str = (String) row.get(timeField);
                    tRefStr = (String) row.get(timeField);
                } else {
                    t1Str = (String) row.get(timeField);
                    t2Str = (String) preCleaned.get(i - 1).get(timeField);
                    tRefStr = (String) intervalCleanedDataRows.getLast().get(timeField);
                }
//                if(t1Str.equals(t2Str)) {
//                    continue;
//                }

                LocalDateTime t1 = DateTimeUtil.getLocalDateTime(t1Str);
                LocalDateTime t2 = DateTimeUtil.getLocalDateTime(t2Str);
                LocalDateTime tRef = DateTimeUtil.getLocalDateTime(tRefStr);

                long intervalSecondRef = Duration.between(tRef, t1).toSeconds();
                long intervalSecond = Duration.between(t1, t2).toSeconds();

                //check restart complete
                if (detectRestartEvent && restartDetectionCounter > 0) {
                    if (intervalSecond > restartIntervalThreadUpper) {
                        //restart complete
                        restartDetectionCounter = 0;
                        isCrossIntervalReadingAdded = false;
                        logger.info("restart completed at " + t1Str);
                    }
                }

                boolean rowAdded = false;
                if (intervalSecondRef > thresholdSecond) {
                    if (!detectRestartEvent || restartDetectionCounter == 0) {
                        intervalCleanedDataRows.add(row);
                        rowAdded = true;
                    }
                }

                if (detectRestartEvent && !rowAdded) {
                    if (intervalSecond > restartIntervalThreadLower && intervalSecond < restartIntervalThreadUpper) {
                        restartDetectionCounter++;
                        logger.info("restarting at " + t1Str + ", counter = " + restartDetectionCounter);
                        if (!isCrossIntervalReadingAdded) {
                            //last cleaned row
                            long intervalMinutes = Duration.between(tRef, t1).toMinutes();
                            if (intervalMinutes == dominantIntervalMinute) {
                                // this row during restart hits the interval
                                intervalCleanedDataRows.add(row);
                                isCrossIntervalReadingAdded = true;
                                if (restartDetectionCounter > 5 && restartDetectionCounter < 21) {
                                    row.put("restart", 1);
                                }
                            }
                        }
                    }
                }
            }
            //reverse the list
            Collections.reverse(intervalCleanedDataRows);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("cleaned_data", intervalCleanedDataRows);
        result.put("dominant_interval", dominantIntervalMinute);
        result.put("outlier_count", outliers);
        result.put("max_interval", maxInterval);

        return result;
    }
    private static long insertMultiPartEstimate2(List<IotHistoryRowDto2> iotHistory, double dominantIntervalMinutes, boolean insertZero) {
        int totalInsertCount = 0;
        for (int i = 0; i < iotHistory.size(); i++) {
            if(i==iotHistory.size()-1) {
                continue;
            }

            IotHistoryRowDto2 rowDto = iotHistory.get(i);
            LocalDateTime timestamp = rowDto.getDt();
            double intervalSecond = rowDto.getRiS();

//            if (Math.abs(intervalMinutes - dominantIntervalMinutes) < dominantIntervalMinutes * 0.8) {
            //skip insert estimate if the interval is less than 1.8 times of the dominant interval
            double threshold = 0.8;
            if(intervalSecond < (1 + threshold) * dominantIntervalMinutes *60) {
                continue;
            }

            LocalDateTime prevTimestamp = iotHistory.get(i+1).getDt();
            Duration duration = Duration.between(prevTimestamp, timestamp);
            long insertCount = Math.floorDiv(Math.round(threshold * duration.toMinutes()), Math.round(dominantIntervalMinutes));
            totalInsertCount += (int) insertCount;

//            if(insertCount == 2){
//                System.out.println("insertCount = 2");
//            }

            // change the original row
            // for reading_total,
            // the original value will not be changed
            // the inserted value will be the linearly interpolated value between the original value and the next value
            // for reading_diff,
            // the original value will be changed to the estimated value
            rowDto.setIsEst(1);
            rowDto.setRiS(dominantIntervalMinutes*60);
            Map<String, Map<String, Object>> prevReading = rowDto.getReadings();
            for (Map.Entry<String, Map<String, Object>> reading : prevReading.entrySet()) {
                Map<String, Object> readingPart = reading.getValue();
                double readingTotal = MathUtil.ObjToDouble(readingPart.get("rt"));
                double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
                double estReadingDiff = readingDiff * dominantIntervalMinutes * 60 / intervalSecond;
//                double estReadingTotal = readingTotal - estReadingDiff;
                readingPart.put("rt", readingTotal);
                readingPart.put("rt_is_est", 0);
//                readingMap.put("rd", estReadingDiff);
                if(insertZero) {
                    readingPart.put("rd", readingDiff);
                }else{
                    readingPart.put("rd", MathUtil.round(estReadingDiff, 5));
                    readingPart.put("rd_is_est", 1);
                }
            }

            //insert estimated rows
            List<IotHistoryRowDto2> insertList = new ArrayList<>();
            LocalDateTime prevEstTimestamp = timestamp;

            for (int j = 0; j < insertCount; j++) {
                LocalDateTime estTimestamp = prevEstTimestamp.minusMinutes(Math.round(dominantIntervalMinutes));
                //insert multi part reading
                Map<String, Map<String, Object>> insertedReading = new HashMap<>();
                for (Map.Entry<String, Map<String, Object>> readingPart : prevReading.entrySet()) {
                    String readingKey = readingPart.getKey();
                    Map<String, Object> prevReadingPart = readingPart.getValue();
                    double readingTotal = MathUtil.ObjToDouble(prevReadingPart.get("rt"));
                    double readingDiff = MathUtil.ObjToDouble(prevReadingPart.get("rd"));
//                    double estReadingDiff = readingDiff * dominantIntervalMinutes / intervalMinutes;

//                    boolean readingTotalIsEst = prevReadingMap.get("reading_total_is_est") != null && (Boolean) prevReadingMap.get("reading_total_is_est");
//                    // the first (original) reading total is not estimated
//                    // therefore should not offset using estReadingDiff
//                    // instead use the readingDiff for original row updated before the loop
//                    if(!readingTotalIsEst){
//                        estReadingDiff = readingDiff;
//                    }
                    double estReadingTotal = readingTotal - readingDiff;

                    Map<String, Object> readingPartInsert = new HashMap<>();

//                    readingMap.put("rd", readingDiff);
                    if(insertZero){
                        readingPartInsert.put("rt", readingTotal);
                        readingPartInsert.put("rd", 0D);
                        readingPartInsert.put("rd_is_insert_zero", 1);
                    }else {
                        readingPartInsert.put("rt", estReadingTotal);
                        readingPartInsert.put("rt_is_est", 1);
                        readingPartInsert.put("rd", MathUtil.round(readingDiff, 5));
                        readingPartInsert.put("rd_is_est", 1);
                    }

                    insertedReading.put(readingKey, readingPartInsert);
                }
                IotHistoryRowDto2 estRowDto = IotHistoryRowDto2.builder()
                        .dt(estTimestamp)
                        .readings(insertedReading)//prevReading)
//                        .riM(dominantIntervalMinutes)
                        .riS(MathUtil.round(dominantIntervalMinutes*60, 3))
                        .isEst(1)
                        .build();
                insertList.add(estRowDto);

                prevEstTimestamp = estTimestamp;
                prevReading = insertedReading;
            }
            iotHistory.addAll(i + 1, insertList);
        }
        return totalInsertCount;
    }

    private static long normalizeNegativeReading(List<IotHistoryRowDto2> iotHistory) {
        //normalize negative reading by offsetting with positive reading
//        int totalOffsetCount = 0;
        int totalNegCount = 0;

        //build diffs map to calculate average diff for each reading part
        Map<String, List<Double>> diffsMap = new HashMap<>();
        for (IotHistoryRowDto2 rowDto : iotHistory) {
            Map<String, Map<String, Object>> readings = rowDto.getReadings();
            for (Map.Entry<String, Map<String, Object>> reading : readings.entrySet()) {
                String readingKey = reading.getKey();
                Map<String, Object> readingPart = reading.getValue();
                double readingTotal = MathUtil.ObjToDouble(readingPart.get("rt"));
                double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
                if (!diffsMap.containsKey(readingKey)) {
                    diffsMap.put(readingKey, new ArrayList<>());
                }
                diffsMap.get(readingKey).add(readingDiff);
            }
        }
        //fine average diff for each reading part
        Map<String, Double> avgDiffMap = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : diffsMap.entrySet()) {
            String readingKey = entry.getKey();
            List<Double> readingDiffs = entry.getValue();
            double avgDiff = MathUtil.findAverage(readingDiffs);
            avgDiffMap.put(readingKey, avgDiff);
        }

        // offset negative reading
        for (int i = 0; i < iotHistory.size(); i++) {
//            if(i==101){
//                System.out.println("i=101");
//            }
            IotHistoryRowDto2 rowDto = iotHistory.get(i);
            Map<String, Map<String, Object>> readings = rowDto.getReadings();
            int maxDistance = 6;
            double spikeThreshold = 8;
            for (Map.Entry<String, Map<String, Object>> reading : readings.entrySet()) {
                String readingKey = reading.getKey();
                Map<String, Object> readingPart = reading.getValue();
                double readingTotal = MathUtil.ObjToDouble(readingPart.get("rt"));
                double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
                boolean isNeg = readingDiff < 0;
                boolean isSpike = readingDiff > spikeThreshold * avgDiffMap.get(readingKey);
                if(!isNeg && !isSpike) {
                    continue;
                }
                totalNegCount++;

                // if the reading is negative/spike, find a spike/neg to offset with the following rules
                // 1. within x intervals before or after
                // 2. reading is 3 times larger than average diff
                // 3. the reading is not used to offset another reading

                for(int j=1; j<maxDistance; j++) {
                    if(i+j < iotHistory.size()) {
                        IotHistoryRowDto2 rowDtoLeft = iotHistory.get(i + j);
                        Map<String, Map<String, Object>> readingsLeft = rowDtoLeft.getReadings();
                        Map<String, Object> readingPartLeft = readingsLeft.get(readingKey);
                        double readingDiffLeft = MathUtil.ObjToDouble(readingPartLeft.get("rd"));
                        boolean foundOnLeft = true;

                        if (isNeg && readingDiffLeft <= 0  ) {
                            foundOnLeft = false;
                        }
                        if(isSpike && readingDiffLeft >= 0){
                            foundOnLeft = false;
                        }
                        double avgDiffLeft = avgDiffMap.get(readingKey);
                        if (isNeg && readingDiffLeft < spikeThreshold * avgDiffLeft) {
                            foundOnLeft = false;
                        }
                        if (foundOnLeft) {
                            //offset
                            double afterOffset = 0.5 * (readingDiffLeft + readingDiff);
                            //found on the left, use the right reading total and add the offset
                            readingPart.put("rd", MathUtil.round(afterOffset, 5));
                            readingPart.put("rd_is_est", 1);
                            readingPartLeft.put("rd", MathUtil.round(afterOffset, 5));
                            readingPartLeft.put("rd_is_est", 1);
                            readingPartLeft.put("left_distance", j);
                            break;
                        }
                    }
                    if(i-j >= 0) {
                        IotHistoryRowDto2 rowDtoRight = iotHistory.get(i - j);
                        Map<String, Map<String, Object>> readingsRight = rowDtoRight.getReadings();
                        Map<String, Object> readingPartRight = readingsRight.get(readingKey);
                        double readingDiffRight = MathUtil.ObjToDouble(readingPartRight.get("rd"));
//                        double readingTotalRight = MathUtil.ObjToDouble(readingPartRight.get("rt"));
                        boolean foundOnRight = true;
                        if (isNeg && readingDiffRight <= 0) {
                            foundOnRight = false;
                        }
                        if(isSpike && readingDiffRight >= 0){
                            foundOnRight = false;
                        }
                        double avgDiffRight = avgDiffMap.get(readingKey);
                        if (isNeg && readingDiffRight < spikeThreshold * avgDiffRight) {
                            foundOnRight = false;
                        }
                        if (foundOnRight) {
                            //offset
                            double afterOffset = 0.5 * (readingDiffRight + readingDiff);
                            readingPart.put("rd", MathUtil.round(afterOffset, 5));
                            readingPart.put("rd_is_est", 1);
                            readingPartRight.put("rd", MathUtil.round(afterOffset, 5));
                            readingPartRight.put("rd_is_est", 1);
                            readingPartRight.put("right_distance", j);
                            break;
                        }
                    }
                }
            }
        }
        //if the latest reading (0) is still a neg or spike, remove it
        IotHistoryRowDto2 rowDto = iotHistory.getFirst();
        Map<String, Map<String, Object>> readings = rowDto.getReadings();
        double spikeThreshold = 3.5;
        for (Map.Entry<String, Map<String, Object>> reading : readings.entrySet()) {
            String readingKey = reading.getKey();
            Map<String, Object> readingPart = reading.getValue();
            double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
            double avgDiff = avgDiffMap.get(readingKey);
            if(readingDiff < 0 || readingDiff > spikeThreshold * avgDiff) {
                //remove the reading
                iotHistory.removeFirst();
                break;
            }
        }

        //check edge neg
        //if the earliest reading is still a neg or spike, remove it
        rowDto = iotHistory.getLast();
        readings = rowDto.getReadings();
        for (Map.Entry<String, Map<String, Object>> reading : readings.entrySet()) {
            String readingKey = reading.getKey();
            Map<String, Object> readingPart = reading.getValue();
            double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
            double avgDiff = avgDiffMap.get(readingKey);
            if( readingDiff < 0 || readingDiff > spikeThreshold * avgDiff) {
                //remove the reading
                iotHistory.removeLast();
                break;
            }
        }
        //if the latest reading (0) is still a neg or spike, remove it
        rowDto = iotHistory.getFirst();
        readings = rowDto.getReadings();
        for (Map.Entry<String, Map<String, Object>> reading : readings.entrySet()) {
            String readingKey = reading.getKey();
            Map<String, Object> readingPart = reading.getValue();
            double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
            double avgDiff = avgDiffMap.get(readingKey);
            if(readingDiff < 0 || readingDiff > spikeThreshold * avgDiff) {
                //remove the reading
                iotHistory.removeFirst();
                break;
            }
        }

        //loop one more time to average out the remaining spikes and negs
        for (int i = 0; i < iotHistory.size(); i++) {
            IotHistoryRowDto2 rowDto2 = iotHistory.get(i);
            Map<String, Map<String, Object>> readings2 = rowDto2.getReadings();
            for (Map.Entry<String, Map<String, Object>> reading : readings2.entrySet()) {
                String readingKey = reading.getKey();
                Map<String, Object> readingPart = reading.getValue();
                double readingDiff = MathUtil.ObjToDouble(readingPart.get("rd"));
                double readingTotal = MathUtil.ObjToDouble(readingPart.get("rt"));
                double avgDiff = avgDiffMap.get(readingKey);

                if(readingDiff > spikeThreshold * avgDiff || readingDiff < 0) {
                    if(i>1 && i<iotHistory.size()-1){
                        double readingDiffLeft = MathUtil.ObjToDouble(iotHistory.get(i-1).getReadings().get(readingKey).get("rd"));
//                        double readingTotalLeft = MathUtil.ObjToDouble(iotHistory.get(i-1).getReadings().get(readingKey).get("rt"));
                        double readingDiffRight = MathUtil.ObjToDouble(iotHistory.get(i+1).getReadings().get(readingKey).get("rd"));
                        double readingTotalRight = MathUtil.ObjToDouble(iotHistory.get(i+1).getReadings().get(readingKey).get("rt"));
                        double afterOffset = 0.5 * (readingDiffLeft + readingDiffRight);
//                        double afterOffsetTotal = 0.5 * (readingTotalLeft + readingTotalRight);
                        readingPart.put("rd", MathUtil.round(afterOffset, 5));
                        readingPart.put("rd_is_est", 1);
                    }
                }
            }
        }
        return totalNegCount;
    }

    private static long insertEstimate(List<IotHistoryRowDto> iotHistory, long dominantIntervalMinutes) {
        int totalInsertCount = 0;
        for (int i = 0; i < iotHistory.size(); i++) {
            if(i==iotHistory.size()-1) {
                continue;
            }

            IotHistoryRowDto rowDto = iotHistory.get(i);
            LocalDateTime timestamp = rowDto.getReadingTimestamp();
            double intervalMinutes = rowDto.getReadingInterval()/DateTimeUtil.oneMinute;
            double kwhDiff = rowDto.getReadingDiff();

            if (Math.abs(intervalMinutes - dominantIntervalMinutes) < dominantIntervalMinutes * 1.99) {
                continue;
            }

            LocalDateTime prevTimestamp = iotHistory.get(i+1).getReadingTimestamp();
            Duration duration = Duration.between(prevTimestamp, timestamp);
            long insertCount = Math.floorDiv(duration.toMinutes(), dominantIntervalMinutes);
            totalInsertCount += (int) insertCount;
            double estKwhDiff = kwhDiff * dominantIntervalMinutes / intervalMinutes;
            //change the original row
            rowDto.setReadingInterval(dominantIntervalMinutes*DateTimeUtil.oneMinute);
            rowDto.setReadingDiff(estKwhDiff);
            rowDto.setEstimated(true);

            LocalDateTime prevEstTimestamp = timestamp;
            double prevKwhTotal = rowDto.getReadingTotal();
            List<IotHistoryRowDto> insertList = new ArrayList<>();
            for (int j = 0; j < insertCount; j++) {
                LocalDateTime estTimestamp = prevEstTimestamp.minusMinutes(dominantIntervalMinutes);
                prevEstTimestamp = estTimestamp;
                if(estTimestamp.isBefore(prevTimestamp.plusMinutes(Math.round(dominantIntervalMinutes*0.5)))) {
                    break;
                }
                double estKwhTotal = prevKwhTotal - estKwhDiff;
                prevKwhTotal = estKwhTotal;
                IotHistoryRowDto estRowDto = IotHistoryRowDto.builder()
                        .readingTimestamp(estTimestamp)
                        .readingTotal(estKwhTotal)
                        .readingDiff(estKwhDiff)
                        .readingInterval(dominantIntervalMinutes*DateTimeUtil.oneMinute)
                        .isEstimated(true)
                        .build();
                insertList.add(estRowDto);
            }
            //invert the list
//            Collections.reverse(insertList);
            iotHistory.addAll(i+1, insertList);

            //delete the original row
//            iotHistory.remove(i);
        }
        return totalInsertCount;
    }

    public static Map<String, IotHistoryDto> consolidateMultiPartReadingDto(
            List<IotHistoryRowDto2> readingHistory, long dominantIntervalMinute,
            String targetIntervalStr,
            int algo){
        if(targetIntervalStr.equalsIgnoreCase("hourly")) {
            if (dominantIntervalMinute > 30) {
                return null;
            }
        }
        if(targetIntervalStr.equalsIgnoreCase("daily")) {
            if (dominantIntervalMinute > 720) {
                return null;
            }
        }
        List<String> parts = new ArrayList<>(readingHistory.getFirst().getReadings().keySet());
        //iterate through the reading history and find the last reading of each hour
        List<IotHistoryRowDto2> consolidatedReadingHistory = new ArrayList<>();

        if (algo == 1) {
            // algo 1: find the last reading of the hour/day
            for (int i = 0; i < readingHistory.size(); i++) {

                if (i == 0) {
                    continue;
                }
                IotHistoryRowDto2 rowPrev = readingHistory.get(i);
                IotHistoryRowDto2 row = readingHistory.get(i - 1);
                LocalDateTime dt = row.getDt();
                LocalDateTime dtPrev = rowPrev.getDt();
                int matchTimeTarget = 0;
                int matchTimeTargetPrev = 0;
                if (targetIntervalStr.equalsIgnoreCase("hourly")) {
                    matchTimeTarget = dt.getHour();
                    matchTimeTargetPrev = dtPrev.getHour();
                } else if (targetIntervalStr.equalsIgnoreCase("daily")) {
                    matchTimeTarget = dt.getDayOfYear();
                    matchTimeTargetPrev = dtPrev.getDayOfYear();
                } else {
                    return null;
                }

                if (matchTimeTarget != matchTimeTargetPrev) {
                    // add the last reading of the hour/day
                    // use deep copy
                    Map<String, Map<String, Object>> readings = new HashMap<>();
                    for (String part : parts) {
                        Map<String, Object> reading = new HashMap<>();
                        Map<String, Object> singleItemReadingPart = rowPrev.getReadings().get(part);
                        reading.put("rt", singleItemReadingPart.get("rt"));
                        reading.put("rt_is_est", singleItemReadingPart.get("rt_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rt_is_est"));
                        reading.put("rd", singleItemReadingPart.get("rd"));
                        reading.put("rd_is_est", singleItemReadingPart.get("rd_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rd_is_est"));
                        readings.put(part, reading);
                    }
                    IotHistoryRowDto2 rowDto = IotHistoryRowDto2.builder()
                            .dt(dtPrev)
                            .readings(readings)
//                            .riS(rowPrev.getRiS())
                            .isEst(rowPrev.getIsEst())
                            .isEmpty(rowPrev.getIsEmpty())
                            .build();
                    consolidatedReadingHistory.add(rowDto);
                }
//            //edge case: the first reading of the history
//            if(i == readingHistory.size()-1){
//                // add the last reading of the hour/day
//                // use deep copy
//                Map<String, Map<String, Object>> readings = new HashMap<>();
//                for (String part : parts) {
//                    Map<String, Object> reading = new HashMap<>();
//                    Map<String, Object> singleItemReadingPart = row.getReadings().get(part);
//                    reading.put("rt", singleItemReadingPart.get("rt"));
//                    reading.put("rt_is_est", singleItemReadingPart.get("rt_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rt_is_est"));
//                    reading.put("rd", singleItemReadingPart.get("rd"));
//                    reading.put("rd_is_est", singleItemReadingPart.get("rd_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rd_is_est"));
//                    readings.put(part, reading);
//                }
//                IotHistoryRowDto2 rowDto = IotHistoryRowDto2.builder()
//                        .dt(dt)
//                        .readings(readings)
//                        .riS(row.getRiS())
//                        .isEst(row.getIsEst())
//                        .build();
//                consolidatedReadingHistory.add(rowDto);
//            }
            }
            // go thru the hourly/daily readings and re-calculate the rd and update
            for (int ihr = 0; ihr < consolidatedReadingHistory.size(); ihr++) {
                if (ihr == consolidatedReadingHistory.size() - 1) continue;

                IotHistoryRowDto2 rowDto = consolidatedReadingHistory.get(ihr);
                IotHistoryRowDto2 rowDtoPrev = consolidatedReadingHistory.get(ihr + 1);

                for (Map.Entry<String, Map<String, Object>> reading : rowDto.getReadings().entrySet()) {
                    Map<String, Object> readingPart = reading.getValue();
                    double rt = MathUtil.ObjToDouble(readingPart.get("rt"));
                    double rtPrev = rowDtoPrev.getReadingPart(reading.getKey(), "rt");

                    double newDiff = rt - rtPrev;
                    //update the rd with the new diff
                    rowDto.setReadingPart(reading.getKey(), "rd", newDiff);
                }
            }
            //remove the last reading
            consolidatedReadingHistory.removeLast();
        }else {
            // algo 2: sum up the readings of the hour/day
            // this algo avoids using single total reading for each consolidated period,
            // single total reading may be inaccurate due to the spike/error
            // instead use the sum of the reading diffs of the period
            IotHistoryRowDto2 consolidatedRow = null;
            for (int i = 0; i < readingHistory.size(); i++) {

                if (i == 0) {
                    continue;
                }
                IotHistoryRowDto2 rowPrev = readingHistory.get(i);
                IotHistoryRowDto2 row = readingHistory.get(i - 1);
                LocalDateTime dt = row.getDt();
                LocalDateTime dtPrev = rowPrev.getDt();
                int matchTimeTarget = 0;
                int matchTimeTargetPrev = 0;
                if (targetIntervalStr.equalsIgnoreCase("hourly")) {
                    matchTimeTarget = dt.getHour();
                    matchTimeTargetPrev = dtPrev.getHour();
                } else if (targetIntervalStr.equalsIgnoreCase("daily")) {
                    matchTimeTarget = dt.getDayOfYear();
                    matchTimeTargetPrev = dtPrev.getDayOfYear();
                } else {
                    return null;
                }

                Map<String, Map<String, Object>> readings = new HashMap<>();

                //edge case: the first reading of the history
                if(consolidatedRow == null){
                    readings = new HashMap<>();
                    for (String part : parts) {
                        Map<String, Object> reading = new HashMap<>();
                        Map<String, Object> singleItemReadingPart = row.getReadings().get(part);
                        reading.put("rt", singleItemReadingPart.get("rt"));
                        reading.put("rt_is_est", singleItemReadingPart.get("rt_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rt_is_est"));
                        reading.put("rd", singleItemReadingPart.get("rd"));
                        reading.put("rd_is_est", singleItemReadingPart.get("rd_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rd_is_est"));
                        readings.put(part, reading);
                    }
                    consolidatedRow = IotHistoryRowDto2.builder()
                            .dt(dt)
                            .readings(readings)
                            .riS(row.getRiS())
                            .isEst(row.getIsEst())
                            .isEmpty(row.getIsEmpty())
                            .build();
                    continue;
                }

                if(matchTimeTarget == matchTimeTargetPrev) {
                    //add the reading to the consolidated row
                    for (String part : parts) {
                        Map<String, Object> singleItemReadingPart = row.getReadings().get(part);
                        double rt = MathUtil.ObjToDouble(singleItemReadingPart.get("rt"));
                        double rd = MathUtil.ObjToDouble(singleItemReadingPart.get("rd"));
                        Map<String, Object> consolidatedReadingPart = consolidatedRow.getReadings().get(part);
                        double rtConsolidated = MathUtil.ObjToDouble(consolidatedReadingPart.get("rt"));
                        double rdConsolidated = MathUtil.ObjToDouble(consolidatedReadingPart.get("rd"));
                        consolidatedReadingPart.put("rt", rt/* + rtConsolidated*/);
                        consolidatedReadingPart.put("rd", rd + rdConsolidated);
//                        readings.put(part, consolidatedReadingPart);
                    }
                }else{
                    //add the consolidated row to the consolidated history

//                  consolidatedRow.setDt(dtPrev);
                    consolidatedRow.setDt(dt);

                    consolidatedReadingHistory.add(consolidatedRow);
                    //create a new consolidated row
                    readings = new HashMap<>();
                    for (String part : parts) {
                        Map<String, Object> reading = new HashMap<>();
                        Map<String, Object> singleItemReadingPart = row.getReadings().get(part);
                        reading.put("rt", singleItemReadingPart.get("rt"));
                        reading.put("rt_is_est", singleItemReadingPart.get("rt_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rt_is_est"));
                        reading.put("rd", singleItemReadingPart.get("rd"));
                        reading.put("rd_is_est", singleItemReadingPart.get("rd_is_est") == null ? 2 : (Integer) singleItemReadingPart.get("rd_is_est"));
                        readings.put(part, reading);
                    }
                    consolidatedRow = IotHistoryRowDto2.builder()
                            .dt(dt)
                            .readings(readings)
                            .riS(row.getRiS())
                            .isEst(row.getIsEst())
                            .isEmpty(row.getIsEmpty())
                            .build();
                }

                //edge case: the last reading of the history
//                if(i == readingHistory.size()-1){
//                    //add the consolidated row to the consolidated history
//                    consolidatedRow.setDt(dt);
//                    consolidatedReadingHistory.add(consolidatedRow);
//                }
            }
        }
        if(consolidatedReadingHistory.isEmpty()){
            return null;
        }

        Map<String, IotHistoryMetaDto> metaMap = new HashMap<>();

        for (String part : parts) {
            LocalDateTime end = consolidatedReadingHistory.getFirst().getDt();
            LocalDateTime start = consolidatedReadingHistory.getLast().getDt();
            long duration = Duration.between(start, end).toMillis();

            List<Double> partReading = consolidatedReadingHistory.stream().map(IotHistoryRowDto2::getReadings).map(m -> m.get(part)).map(m -> MathUtil.ObjToDouble(m.get("rt"))).collect(Collectors.toList());
            double averageReading = MathUtil.findAverage(partReading);
            double medianVal = MathUtil.findMedian(partReading);
            double maxVal = MathUtil.findMax(partReading);
            double minVal = MathUtil.findMin(partReading);
            double minValNonZero = MathUtil.findMinNonZero(partReading);
            double total = MathUtil.findTotal(partReading);
            long totalPositive = MathUtil.findPositiveCount(partReading);
            long consolidatedDominantIntervalMinutes = 0;
            if(targetIntervalStr.equalsIgnoreCase("hourly")) {
                consolidatedDominantIntervalMinutes = 60;
            }else if(targetIntervalStr.equalsIgnoreCase("daily")){
                consolidatedDominantIntervalMinutes = 1440;
            }
            IotHistoryMetaDto meta = IotHistoryMetaDto.builder()
                    .dominantInterval(consolidatedDominantIntervalMinutes * DateTimeUtil.oneMinute)
                    .duration(duration)
                    .avgVal(averageReading)
                    .medianVal(medianVal)
                    .total(total)
                    .positiveCount(totalPositive)
                    .maxVal(maxVal)
                    .minVal(minVal)
                    .minValNonZero(minValNonZero)
                    .build();
            metaMap.put(part, meta);

            List<Double> partDiffs = consolidatedReadingHistory.stream().map(IotHistoryRowDto2::getReadings).map(m -> m.get(part)).map(m -> MathUtil.ObjToDouble(m.get("rd"))).collect(Collectors.toList());
            double averageReadingDiffs = MathUtil.findAverage(partDiffs);
            double medianValDiffs = MathUtil.findMedian(partDiffs);
            double maxValDiffs = MathUtil.findMax(partDiffs);
            double minValDiffs = MathUtil.findMin(partDiffs);
            double minValNonZeroDiffs = MathUtil.findMinNonZero(partDiffs);
            double totalDiffs = MathUtil.findTotal(partDiffs);
            long totalPositiveDiffs = MathUtil.findPositiveCount(partDiffs);

            IotHistoryMetaDto metaDiff = IotHistoryMetaDto.builder()
                    .dominantInterval(consolidatedDominantIntervalMinutes * DateTimeUtil.oneMinute)
                    .duration(duration)
                    .avgVal(averageReadingDiffs)
                    .medianVal(medianValDiffs)
                    .total(totalDiffs)
                    .positiveCount(totalPositiveDiffs)
                    .maxVal(maxValDiffs)
                    .minVal(minValDiffs)
                    .minValNonZero(minValNonZeroDiffs)
                    .build();
            metaMap.put(part+"_diff", metaDiff);
        }

        IotHistoryDto consolidatedIotHistoryDto = IotHistoryDto.builder()
                .history2(consolidatedReadingHistory)
                .metas(metaMap)
                .build();
        return Map.of("consolidated_history", consolidatedIotHistoryDto);
    }
    public static Map<String, IotHistoryMetaDto> genMultiPartMetas(
            List<IotHistoryRowDto2> readingHistory, List<String> dataFieldList,
            long dominantIntervalMinute){
        Map<String, IotHistoryMetaDto> metaMap = new HashMap<>();
        LocalDateTime start = readingHistory.getLast().getDt();
        LocalDateTime end = readingHistory.getFirst().getDt();
        long duration = Duration.between(start, end).toMillis();
        for (String part : dataFieldList) {
            Double firstReadingTotal = MathUtil.ObjToDouble(readingHistory.getLast().getReadings().get(part).get("rt"));
            LocalDateTime firstReadingDt = readingHistory.getLast().getDt();
            Double lastReadingTotal = MathUtil.ObjToDouble(readingHistory.getFirst().getReadings().get(part).get("rt"));
            LocalDateTime lastReadingDt = readingHistory.getFirst().getDt();
            Double firstReadingDiff = MathUtil.ObjToDouble(readingHistory.getLast().getReadings().get(part).get("rd"));
            LocalDateTime firstReadingDiffDt = readingHistory.getLast().getDt();
            Double lastReadingDiff = MathUtil.ObjToDouble(readingHistory.getFirst().getReadings().get(part).get("rd"));
            LocalDateTime lastReadingDiffDt = readingHistory.getFirst().getDt();

            double minTotal = Double.MAX_VALUE;
            double maxTotal = -1* Double.MAX_VALUE;
            LocalDateTime minTotalDt = null;
            LocalDateTime maxTotalDt = null;
            double minDiff = Double.MAX_VALUE;
            double maxDiff = -1* Double.MAX_VALUE;
            LocalDateTime minDiffDt = null;
            LocalDateTime maxDiffDt = null;
            for (IotHistoryRowDto2 iotHistoryRowDto : readingHistory) {
                LocalDateTime dt = iotHistoryRowDto.getDt();
                Map<String, Map<String, Object>> readings = iotHistoryRowDto.getReadings();
                Double total = MathUtil.ObjToDouble(readings.get(part).get("rt"));
                Double diff = MathUtil.ObjToDouble(readings.get(part).get("rd"));
                if(total == null || diff == null){
                    continue;
                }

                if(total < minTotal){
                    minTotal = total;
                    minTotalDt = dt;
                }
                if(total > maxTotal){
                    maxTotal = total;
                    maxTotalDt = dt;
                }

                if(diff < minDiff){
                    minDiff = diff;
                    minDiffDt = dt;
                }
                if(diff > maxDiff){
                    maxDiff = diff;
                    maxDiffDt = dt;
                }
            }
            List<Double> partTotals = readingHistory.stream()
                    .map(IotHistoryRowDto2::getReadings)
                    .map(m -> m.get(part))
                    .map(m -> MathUtil.ObjToDouble(m.get("rt")))
                    .collect(Collectors.toList());
            List<Double> partDiffs = readingHistory.stream()
                    .map(IotHistoryRowDto2::getReadings)
                    .map(m -> m.get(part))
                    .map(m -> MathUtil.ObjToDouble(m.get("rd")))
                    .collect(Collectors.toList());
            Double avgTotal = MathUtil.findAverage(partTotals);
            Double medianTotal = MathUtil.findMedian(partTotals);
            Double avgDiff = MathUtil.findAverage(partDiffs);
            //find the first and last total and calculate the average diff
//            double avgDiff = (lastReadingTotal - firstReadingTotal) / /*(readingHistory.size()-1)*/ readingHistory.size();
            Double medianDiff = MathUtil.findMedian(partDiffs);

            IotHistoryMetaDto metaTotal = IotHistoryMetaDto.builder()
                    .duration(duration)
                    .dominantInterval(dominantIntervalMinute * DateTimeUtil.oneMinute)
                    .firstVal(firstReadingTotal)
                    .firstValDt(firstReadingDt)
                    .lastVal(lastReadingTotal)
                    .lastValDt(lastReadingDt)
                    .minVal(minTotal)
                    .minValNonZeroDt(minTotalDt)
                    .maxVal(maxTotal)
                    .maxValDt(maxTotalDt)
                    .avgVal(avgTotal)
                    .medianVal(medianTotal)
                    .build();
            IotHistoryMetaDto metaDiff = IotHistoryMetaDto.builder()
                    .duration(duration)
                    .dominantInterval(dominantIntervalMinute * DateTimeUtil.oneMinute)
                    .firstVal(firstReadingDiff)
                    .firstValDt(firstReadingDiffDt)
                    .lastVal(lastReadingDiff)
                    .lastValDt(lastReadingDiffDt)
                    .minVal(minDiff)
                    .minValNonZeroDt(minDiffDt)
                    .maxVal(maxDiff)
                    .maxValDt(maxDiffDt)
                    .avgVal(avgDiff)
                    .medianVal(medianDiff)
                    .build();
            metaMap.put(part, metaTotal);
            metaMap.put(part+"_diff", metaDiff);
        }
        return metaMap;
    }

    public static String genBuildingId(String buildingName, String block){
        if(buildingName==null){
            return "";
        }
        if(buildingName.trim().isEmpty()){
            return "";
        }
        return (block==null? "": block.trim().toLowerCase())+
                "-"+
                buildingName.trim().replace(" ", "-").replace("'", "-").toLowerCase();

    }
}
