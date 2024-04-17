package org.pabuff.evs2helper.job;

import org.pabuff.dto.ItemIdTypeEnum;
import org.pabuff.dto.ItemTypeEnum;
import org.pabuff.evs2helper.locale.LocalHelper;
import org.pabuff.evs2helper.report.TenantUsageProcessor;
import org.pabuff.oqghelper.OqgHelper;
import org.pabuff.oqghelper.QueryHelper;
import org.pabuff.utils.DateTimeUtil;
import org.pabuff.utils.MathUtil;
import org.pabuff.utils.SqlUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;

@Service
public class BillProcessor {
    Logger logger = Logger.getLogger(BillProcessor.class.getName());

    @Autowired
    private LocalHelper localHelper;
    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private QueryHelper queryHelper;
    @Autowired
    private TenantUsageProcessor tenantUsageProcessor;

    final String billingRecTable = "billing_rec_cw";

    public Map<String, Object> genAllTenantBills(String fromDate, String toDate, Boolean isMonthly) {
        logger.info("Processing all tenant bills");

        String tenantInfoQuery = "select tenant_name, tenant_label from tenant";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2(tenantInfoQuery, true);
        } catch (Exception e) {
            logger.severe("Failed to query tenant table: " + e.getMessage());
            return Collections.singletonMap("error", "Failed to query tenant table: " + e.getMessage());
        }
        int tenantCount = resp.size();
        int processedCount = 0;
        List<LinkedHashMap<String, Object>> billResult = new ArrayList<>();
        for (Map<String, Object> tenantInfo : resp) {
            LinkedHashMap<String, Object> result2 = new LinkedHashMap<>();
            result2.put("tenant_name", tenantInfo.get("tenant_name"));
            result2.put("tenant_label", tenantInfo.get("tenant_label"));

            Map<String, Object> result =
            genSingleTenantBillingRec((String) tenantInfo.get("tenant_name"),
                                      fromDate, toDate, isMonthly, null, null, null, null);
            result2.put("result", result);
            billResult.add(result2);

            processedCount++;
            logger.info(processedCount + " / " + tenantCount + " tenants processed");
        }
        return Collections.singletonMap("result", billResult);
    }

    public Map<String, Object> genSingleTenantBillingRec(String tenantName,
                                                         String fromDate, String toDate, Boolean isMonthly,
                                                         Map<String, Object> tpRateInfo,
                                                         Map<String, Object> manualItemInfo,
                                                         Map<String, Object> lineItemInfo,
                                                         String genBy) {
        logger.info("Processing bill");

        Map<String, Object> tenantInfoResult = queryHelper.getItemInfo(tenantName, ItemIdTypeEnum.NAME, ItemTypeEnum.TENANT);
        if(tenantInfoResult == null){
            logger.severe("Tenant not found: " + tenantName);
            return Collections.singletonMap("error", "Tenant not found: " + tenantName);
        }
        if(!tenantInfoResult.containsKey("item_info")){
            logger.severe("No tenant info found for: " + tenantName);
            return Collections.singletonMap("error", "No tenant info found for: " + tenantName);
        }
        Map<String, Object> tenantInfo = (Map<String, Object>) tenantInfoResult.get("item_info");
        Long tenantIndex = MathUtil.ObjToLong(tenantInfo.get("id"));

        Map<String, Object> tenantTariffIds = queryHelper.getTenantTariffPackages(tenantIndex);
        if(tenantTariffIds == null){
            logger.severe("No tariff package found for tenant: " + tenantName);
            return Collections.singletonMap("error", "No tariff package found for tenant: " + tenantName);
        }
        //if all vals are null
        if(tenantTariffIds.values().stream().allMatch(Objects::isNull)){
            logger.severe("No tariff package found for tenant: " + tenantName);
            return Collections.singletonMap("error", "No tariff package found for tenant: " + tenantName);
        }

        String idSelQuery = "SELECT tenant_name FROM tenant WHERE tenant_name = '" + tenantName + "'";

        Map<String, String> tenantRequest = Map.of("is_monthly", isMonthly.toString(),
                                                   "project_scope", "ems_cw_nus",
                                                   "site_scope", "",
                                                   "id_select_query", idSelQuery,
                                                   "start_datetime", fromDate,
                                                   "end_datetime", toDate,
                                                   "item_type", ItemTypeEnum.TENANT.name(),
                                                   "meter_type", ItemTypeEnum.METER_IWOW.name(),
                                                   "item_id_type", ItemIdTypeEnum.NAME.name());
//        if("dgzjc-int-240103-369".equals(tenantName)){
//            logger.info("dgzjc-int-240103-369");
//        }

        Map<String, Object> tenantResult = tenantUsageProcessor.getListUsageSummary(tenantRequest);
        List<Map<String, Object>> tenantListUsageSummary = (List<Map<String, Object>>) tenantResult.get("tenant_list_usage_summary");
        List<Map<String, Object>> tenantUsageSummary = (List<Map<String, Object>>) tenantListUsageSummary.getFirst().get("tenant_usage_summary");

        double totalE = 0D;
        double totalW = 0D;
        double totalB = 0D;
        double totalN = 0D;
        double totalG = 0D;
        Map<String, Object> meterTypeRates = new HashMap<>();
        boolean incompleteUsageData = false;
        for(Map<String, Object> meterGroupUsage : tenantUsageSummary){
            String meterTypeTag = (String) meterGroupUsage.get("meter_type");
            Map<String, Object> tariffResult;
//            if(tpRateInfo !=null && tpRateInfo.containsKey(meterTypeTag)) {
//                tariffResult = (Map<String, Object>) tpRateInfo.get(meterTypeTag);
//            }else{
//                tariffResult = findTariff(meterTypeTag, tenantTariffIds, fromDate, toDate);
//            }
            if(tpRateInfo !=null) {
            //custom billing
                if(genBy == null) {
                    logger.severe("genBy is null");
                    return Collections.singletonMap("error", "genBy is null");
                }
                if(tpRateInfo.containsKey(meterTypeTag)) {
                     tariffResult = (Map<String, Object>) tpRateInfo.get(meterTypeTag);
                }else {
                    logger.info("No tariff supplied for meterTypeTag: " + meterTypeTag);
                    continue;
                }
            }else{
                tariffResult = findTariff(meterTypeTag, tenantTariffIds, fromDate, toDate);
            }
            if(tariffResult.containsKey("error")){
                logger.severe("Failed to find tariff for meterTypeTag: " + meterTypeTag);
                return Collections.singletonMap("error", "Failed to find tariff for meterTypeTag: " + meterTypeTag);
            }
            meterTypeRates.put(meterTypeTag, tariffResult);

            Map<String, Object> meterGroupUsageSummary = (Map<String, Object>) meterGroupUsage.get("meter_group_usage_summary");
            List<Map<String, Object>> meterListUsage = (List<Map<String, Object>>) meterGroupUsageSummary.get("meter_list_usage_summary");
            
            for(Map<String, Object> meterUsageSummary : meterListUsage){
                Object usageObj = meterUsageSummary.get("usage");
                if("-".equals(usageObj.toString())){
                    incompleteUsageData = true;
                    break;
                }
                Double usage = MathUtil.ObjToDouble(usageObj);
                Double percentage = MathUtil.ObjToDouble(meterUsageSummary.get("percentage"));
                double usageShare = usage * percentage/100;
                switch (meterTypeTag){
                    case "E":
                        totalE += usageShare;
                        break;
                    case "W":
                        totalW += usageShare;
                        break;
                    case "B":
                        totalB += usageShare;
                        break;
                    case "N":
                        totalN += usageShare;
                        break;
                    case "G":
                        totalG += usageShare;
                        break;
                }
            }
            if(incompleteUsageData){
                logger.severe("Inconsistent usage data found for tenant: " + tenantName);
                return Collections.singletonMap("error", "Inconsistent usage data found for tenant: " + tenantName);
            }
        }
        //manual usage items
//        if(manualItemInfo!=null){
//            for (Map.Entry<String, Object> entry : manualItemInfo.entrySet()) {
//                String meterTypeTag = entry.getKey();
//                Double usage = MathUtil.ObjToDouble(entry.getValue());
//                Map<String, Object> tariffResult;
//                if(tpRateInfo !=null) {
//                    //custom billing
//                    if(genBy == null) {
//                        logger.severe("genBy is null");
//                        return Collections.singletonMap("error", "genBy is null");
//                    }
//                    if(tpRateInfo.containsKey(meterTypeTag)) {
//                        tariffResult = (Map<String, Object>) tpRateInfo.get(meterTypeTag);
//                    }else {
//                        logger.info("No tariff supplied for meterTypeTag: " + meterTypeTag);
//                        continue;
//                    }
//                }else{
//                    tariffResult = findTariff(meterTypeTag, tenantTariffIds, fromDate, toDate);
//                }
//                if(tariffResult.containsKey("error")){
//                    logger.severe("Failed to find tariff for meterTypeTag: " + meterTypeTag);
//                    return Collections.singletonMap("error", "Failed to find tariff for meterTypeTag: " + meterTypeTag);
//                }
//                meterTypeRates.put(meterTypeTag, tariffResult);
//                switch (meterTypeTag){
//                    case "E":
//                        totalE += usage;
//                        break;
//                    case "W":
//                        totalW += usage;
//                        break;
//                    case "B":
//                        totalB += usage;
//                        break;
//                    case "N":
//                        totalN += usage;
//                        break;
//                    case "G":
//                        totalG += usage;
//                        break;
//                }
//            }
//        }
        Map<String, Object> billResult =
                genBillingRecord(tenantInfo, meterTypeRates,
                        fromDate, toDate, isMonthly,
                        manualItemInfo,
                        lineItemInfo,
                        genBy);
        if(billResult.containsKey("error")){
            logger.severe("Failed to generate bill record: " + billResult.get("error"));
            return Collections.singletonMap("error", "Failed to generate bill record: " + billResult.get("error"));
        }
        logger.info("Bill processed for tenant: " + tenantName + " bill_name" + billResult.get("result"));
        return Collections.singletonMap("result", billResult.get("result"));
    }

    public Map<String, Object> genBillingRecord(Map<String, Object> tenantInfo,
                                                Map<String, Object> meterTypeRates,
                                                String fromTimestamp, String toTimestamp, Boolean isMonthly,
                                                Map<String, Object> manualItemInfo,
                                                Map<String, Object> lineItemInfo,
                                                String genBy) {
        logger.info("Generating bill");
        if(meterTypeRates.isEmpty()){
            logger.warning("Missing meter type rate");
            return Collections.singletonMap("error", "Missing meter type rate");
        }
//        String fromTimestamp = (String) tariffPackageInfo.get("from_timestamp");
//        String toTimestamp = (String) tariffPackageInfo.get("to_timestamp");
//        String rate = (String) tariffPackageInfo.get("rate");
//        String currency = (String) tariffPackageInfo.get("currency");
//
        String tenantIndexStr = (String) tenantInfo.get("id");
        Long tenantIndex = MathUtil.ObjToLong(tenantIndexStr);
        String tenantName = (String) tenantInfo.get("tenant_name");
//
//        Long tariffPackageId = MathUtil.ObjToLong(tariffPackageInfo.get("tariff_package_id"));
//        Long tariffPackageRateId = MathUtil.ObjToLong(tariffPackageInfo.get("id"));
//

//        String billType = "usage";
//        String billCurrency = currency;
//        String billRate = rate;
//        String billFromTimestamp = fromTimestamp;
//        String billToTimestamp = toTimestamp;
//        String billUsage = usage;
//        String billAmount = String.valueOf(Double.parseDouble(rate) * Double.parseDouble(usage));
//        String billPercentage = String.valueOf(percentage);
//
        Map<String, Object> content = new HashMap<>();
        content.put("scope_str", tenantInfo.get("scope_str"));
        content.put("is_monthly", isMonthly);
        content.put("from_timestamp", fromTimestamp);
        content.put("to_timestamp", toTimestamp);
        content.put("tenant_id", tenantIndex);

        if(manualItemInfo!=null){
            for (Map.Entry<String, Object> entry : manualItemInfo.entrySet()) {
                String meterTypeTag = entry.getKey();
                Double usage = MathUtil.ObjToDouble(entry.getValue());
                content.put(meterTypeTag.toLowerCase(), usage);
            }
        }
        if(lineItemInfo!=null){
            if(lineItemInfo.containsKey("line_item_label_1")){
                content.put("line_item_label_1", lineItemInfo.get("line_item_label_1"));
                content.put("line_item_amount_1", lineItemInfo.get("line_item_amount_1"));
            }
        }

        if(genBy != null){
            content.put("gen_type", "manual");
            content.put("gen_by", genBy);
        }else{
            content.put("gen_type", "auto");
        }

        for (Map.Entry<String, Object> entry : meterTypeRates.entrySet()) {
            Map<String, Object> meterTypeRate = (Map<String, Object>) entry.getValue();
            Long tariffPackageRateId = MathUtil.ObjToLong(meterTypeRate.get("id"));
            content.put("tariff_package_rate_id_"+entry.getKey().toLowerCase(), tariffPackageRateId);
        }

        boolean billExists = false;
        String billQuerySql = null;
        try {
            Map<String, String> sqlResult = SqlUtil.makeSelectSql2(
                    Map.of("from", billingRecTable,"targets", content));
            billQuerySql = sqlResult.get("sql");
        }catch (Exception e){
            logger.severe("Failed to generate bill query sql: " + e.getMessage());
            return Collections.singletonMap("error", "Failed to generate bill query sql: " + e.getMessage());
        }
        List<Map<String, Object>> billRecs;
        try {
            billRecs = oqgHelper.OqgR2(billQuerySql, true);
        } catch (Exception e) {
            logger.severe("Failed to query bill: " + e.getMessage());
            return Collections.singletonMap("error", "Failed to query bill: " + e.getMessage());
        }
        if(billRecs.size()==1){
            billExists = true;
        }

        if(billExists){
            logger.info("Bill already exists");
            String billUpdateSql = null;
            try {
                String localNowStr = localHelper.getLocalNowStr();
                content.put("updated_timestamp", localNowStr);

                String billRecIndexStr = (String) billRecs.getFirst().get("id");
                Long billRecIndex = MathUtil.ObjToLong(billRecIndexStr);
                Map<String, String> sqlResult = SqlUtil.makeUpdateSql(
                        Map.of("table", billingRecTable,
                               "target_key", "id",
                               "target_value", billRecIndex,
                               "content", content)
                );
                billUpdateSql = sqlResult.get("sql");
            }catch (Exception e){
                logger.severe("Failed to generate bill update sql: " + e.getMessage());
                return Collections.singletonMap("error", "Failed to generate bill update sql: " + e.getMessage());
            }
            try {
                oqgHelper.OqgIU(billUpdateSql);
            } catch (Exception e) {
                logger.severe("Failed to update bill: " + e.getMessage());
                return Collections.singletonMap("error", "Failed to update bill: " + e.getMessage());
            }
        }else {
            String billName = genBillName(tenantName, fromTimestamp, toTimestamp);
            content.put("name", billName);
            content.put("created_timestamp", localHelper.getLocalNowStr());
            content.put("lc_status", "generated");

            String billInsertSql = null;
            try {
                Map<String, String> sqlResult = SqlUtil.makeInsertSql(Map.of("table", billingRecTable,"content", content));
                billInsertSql = sqlResult.get("sql");
            }catch (Exception e){
                logger.severe("Failed to generate bill insert sql: " + e.getMessage());
                return Collections.singletonMap("error", "Failed to generate bill insert sql: " + e.getMessage());
            }

            try {
                oqgHelper.OqgIU(billInsertSql);
            } catch (Exception e) {
                logger.severe("Failed to insert bill: " + e.getMessage());
                return Collections.singletonMap("error", "Failed to insert bill: " + e.getMessage());
            }
        }

        return Collections.singletonMap("result", content.get("name"));
    }

    public String genBillName(String tenantName, String fromTimestamp, String toTimestamp) {
        LocalDateTime fromTimestampLocal = DateTimeUtil.getLocalDateTime(fromTimestamp);
        LocalDateTime toTimestampLocal = DateTimeUtil.getLocalDateTime(toTimestamp);
        LocalDateTime midTime = fromTimestampLocal.plusSeconds(Duration.between(fromTimestampLocal, toTimestampLocal).getSeconds()/2);
        String yyMM = midTime.getYear() + "-" + midTime.getMonthValue();
        //3 random numbers
        StringBuilder suffix = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            suffix.append((int) (Math.random() * 10));
        }
        return "b" + "-" + tenantName + "-" + yyMM + "-" +  suffix;
    }

    public Map<String, Object> findTariff(String meterTypeTag, Map<String, Object> tenantTariffIds, String firstReadingTimeStr, String lastReadingTimeStr) {
        logger.info("Finding tariff");

        //find the id that ends with the meterTypeTag
        String tariffId = null;
        for (Map.Entry<String, Object> entry : tenantTariffIds.entrySet()) {
            if (entry.getKey().endsWith("_"+meterTypeTag.toLowerCase())) {
                tariffId = entry.getKey();
                break;
            }
        }
        if(tariffId == null){
            logger.severe("Tariff not found for meterTypeTag: " + meterTypeTag);
            return Collections.singletonMap("error", "Tariff not found for meterTypeTag: " + meterTypeTag);
        }

        Long tariffPackageIndex = MathUtil.ObjToLong(tenantTariffIds.get(tariffId));
        Map<String, Object> tariffRateResult = queryHelper.getTariffPackageRates(tariffPackageIndex, 12);
        List<Map<String, Object>> tariffRates = (List<Map<String, Object>>) tariffRateResult.get("tariff_package_rates");
        if(tariffRates == null || tariffRates.isEmpty()){
            logger.severe("No tariff rates found for tariffPackageIndex: " + tariffPackageIndex);
            return Collections.singletonMap("error", "No tariff rates found for tariffPackageIndex: " + tariffPackageIndex);
        }

        for(Map<String, Object> tariffRate : tariffRates){
            //find the tariff that is valid for the firstReadingTime
            String fromTimestampStr = (String) tariffRate.get("from_timestamp");
            LocalDateTime fromTimestamp = DateTimeUtil.getLocalDateTime(fromTimestampStr);
            String toTimestampStr = (String) tariffRate.get("to_timestamp");
            LocalDateTime toTimestamp = DateTimeUtil.getLocalDateTime(toTimestampStr);

            LocalDateTime firstReadingTime = DateTimeUtil.getLocalDateTime(firstReadingTimeStr);
            LocalDateTime lastReadingTime = DateTimeUtil.getLocalDateTime(lastReadingTimeStr);
            LocalDateTime midTime = firstReadingTime.plusSeconds(Duration.between(firstReadingTime, lastReadingTime).getSeconds()/2);

            if(fromTimestamp.isBefore(midTime) && toTimestamp.isAfter(midTime)){
//                tariffRate.put("tariff_package_rate_id_col_name", "tariff_package_rate_id_"+meterTypeTag.toLowerCase());
                return tariffRate;
            }
        }
        logger.severe("No valid tariff found for meterTypeTag: " + meterTypeTag);
        return Collections.singletonMap("error", "No valid tariff found for meterTypeTag: " + meterTypeTag);
    }
}
