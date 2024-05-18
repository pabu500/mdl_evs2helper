package org.pabuff.evs2helper.billing;

import org.pabuff.dto.ItemIdTypeEnum;
import org.pabuff.dto.ItemTypeEnum;
import org.pabuff.evs2helper.locale.LocalHelper;
import org.pabuff.evs2helper.tenant.TenantUsageProcessor;
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
public class BillingProcessor {
    Logger logger = Logger.getLogger(BillingProcessor.class.getName());

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
                    genSingleTenantBill((String) tenantInfo.get("tenant_name"),
                            fromDate, toDate, isMonthly, null, null, null, null);
            result2.put("result", result);
            billResult.add(result2);

            processedCount++;
            logger.info(processedCount + " / " + tenantCount + " tenants processed");
        }
        return Collections.singletonMap("result", billResult);
    }

    // generate bill with auto usage calculation
    // then record billed usage and rate
    public Map<String, Object> genSingleTenantBill(String tenantName,
                                                   String fromDate, String toDate, Boolean isMonthly,
                                                   Map<String, Object> tpRateInfo,
//                                                   Map<String, Object> autoUsageInfo,
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
        List<Map<String, Object>> subTenantListUsageSummary = new ArrayList<>();
        if(tenantListUsageSummary.getFirst().get("sub_tenant_list_usage_summary") != null) {
            subTenantListUsageSummary = (List<Map<String, Object>>) tenantListUsageSummary.getFirst().get("sub_tenant_list_usage_summary");
        }
        Double totalAutoUsageE = null;
        Double totalAutoUsageW = null;
        Double totalAutoUsageB = null;
        Double totalAutoUsageN = null;
        Double totalAutoUsageG = null;
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
//                    continue;
                    // fail fast
                    return Collections.singletonMap("error", "No tariff supplied for meterTypeTag: " + meterTypeTag);
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
                        if(totalAutoUsageE == null){
                            totalAutoUsageE = 0.0;
                        }
                        totalAutoUsageE += usageShare;
                        break;
                    case "W":
                        if(totalAutoUsageW == null){
                            totalAutoUsageW = 0.0;
                        }
                        totalAutoUsageW += usageShare;
                        break;
                    case "B":
                        if(totalAutoUsageB == null){
                            totalAutoUsageB = 0.0;
                        }
                        totalAutoUsageB += usageShare;
                        break;
                    case "N":
                        if(totalAutoUsageN == null){
                            totalAutoUsageN = 0.0;
                        }
                        totalAutoUsageN += usageShare;
                        break;
                    case "G":
                        if(totalAutoUsageG == null){
                            totalAutoUsageG = 0.0;
                        }
                        totalAutoUsageG += usageShare;
                        break;
                }
            }
            if(incompleteUsageData){
                logger.severe("Inconsistent usage data found for tenant: " + tenantName);
                return Collections.singletonMap("error", "Inconsistent usage data found for tenant: " + tenantName);
            }
        }

        Map<String, Object> autoUsage = new HashMap<>();
        autoUsage.put("billed_auto_usage_e", totalAutoUsageE);
        autoUsage.put("billed_auto_usage_w", totalAutoUsageW);
        autoUsage.put("billed_auto_usage_b", totalAutoUsageB);
        autoUsage.put("billed_auto_usage_n", totalAutoUsageN);
        autoUsage.put("billed_auto_usage_g", totalAutoUsageG);

        //sub tenant usage
        Double totalSubTenantUsageE = null;
        Double totalSubTenantUsageW = null;
        Double totalSubTenantUsageB = null;
        Double totalSubTenantUsageN = null;
        Double totalSubTenantUsageG = null;

        for(Map<String, Object> subTenantUsage : subTenantListUsageSummary) {
            String subTenantName = (String) subTenantUsage.get("tenant_name");
            List<Map<String, Object>> subTenantUsageSummary = (List<Map<String, Object>>) subTenantUsage.get("tenant_usage_summary");
            for (Map<String, Object> meterGroupUsage : subTenantUsageSummary) {
                String meterTypeTag = (String) meterGroupUsage.get("meter_type");

                Map<String, Object> meterGroupUsageSummary = (Map<String, Object>) meterGroupUsage.get("meter_group_usage_summary");
                List<Map<String, Object>> meterListUsage = (List<Map<String, Object>>) meterGroupUsageSummary.get("meter_list_usage_summary");

                for (Map<String, Object> meterUsageSummary : meterListUsage) {
                    Object usageObj = meterUsageSummary.get("usage");
                    if ("-".equals(usageObj.toString())) {
                        incompleteUsageData = true;
                        break;
                    }
                    Double usage = MathUtil.ObjToDouble(usageObj);
                    Double percentage = MathUtil.ObjToDouble(meterUsageSummary.get("percentage"));
                    double usageShare = usage * percentage / 100;
                    switch (meterTypeTag) {
                        case "E":
                            if (totalSubTenantUsageE == null) {
                                totalSubTenantUsageE = 0.0;
                            }
                            totalSubTenantUsageE += usageShare;
                            break;
                        case "W":
                            if (totalSubTenantUsageW == null) {
                                totalSubTenantUsageW = 0.0;
                            }
                            totalSubTenantUsageW += usageShare;
                            break;
                        case "B":
                            if (totalSubTenantUsageB == null) {
                                totalSubTenantUsageB = 0.0;
                            }
                            totalSubTenantUsageB += usageShare;
                            break;
                        case "N":
                            if (totalSubTenantUsageN == null) {
                                totalSubTenantUsageN = 0.0;
                            }
                            totalSubTenantUsageN += usageShare;
                            break;
                        case "G":
                            if (totalSubTenantUsageG == null) {
                                totalSubTenantUsageG = 0.0;
                            }
                            totalSubTenantUsageG += usageShare;
                            break;
                    }
                }
            }
        }
        Map<String, Object> subTenantUsage = new HashMap<>();
        subTenantUsage.put("billed_sub_tenant_usage_e", totalSubTenantUsageE);
        subTenantUsage.put("billed_sub_tenant_usage_w", totalSubTenantUsageW);
        subTenantUsage.put("billed_sub_tenant_usage_b", totalSubTenantUsageB);
        subTenantUsage.put("billed_sub_tenant_usage_n", totalSubTenantUsageN);
        subTenantUsage.put("billed_sub_tenant_usage_g", totalSubTenantUsageG);

        Map<String, Object> billResult = genBillingRecord(tenantInfo, meterTypeRates,
                fromDate, toDate, isMonthly,
                autoUsage, subTenantUsage,
                manualItemInfo,
                lineItemInfo,
                genBy);
        if(billResult.containsKey("error")){
            logger.severe("Failed to generate bill record: " + billResult.get("error"));
            return Collections.singletonMap("error", "Failed to generate bill record: " + billResult.get("error"));
        }
        logger.info("Bill processed for tenant: " + tenantName + " bill_name" + billResult.get("result"));
        return billResult;
    }

    private Map<String, Object> findTariff(String meterTypeTag, Map<String, Object> tenantTariffIds, String firstReadingTimeStr, String lastReadingTimeStr) {
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

    // generate billing record
    // billing record contains info on tenant, period, tp, manual usage, line item, but without auto usage calculation
    private Map<String, Object> genBillingRecord(Map<String, Object> tenantInfo,
                                                 Map<String, Object> meterTypeRates,
                                                 String fromTimestamp, String toTimestamp, Boolean isMonthly,
                                                 Map<String, Object> autoItemInfo,
                                                 Map<String, Object> subTenantUsage,
                                                 Map<String, Object> manualItemInfo,
                                                 Map<String, Object> lineItemInfo,
                                                 String genBy) {
        logger.info("Generating bill");
        if(meterTypeRates.isEmpty()){
            logger.warning("Missing meter type rate");
            return Collections.singletonMap("error", "Missing meter type rate");
        }

        String tenantIndexStr = (String) tenantInfo.get("id");
        Long tenantIndex = MathUtil.ObjToLong(tenantIndexStr);
        String tenantName = (String) tenantInfo.get("tenant_name");

        Map<String, Object> content = new HashMap<>();
        content.put("scope_str", tenantInfo.get("scope_str"));
        content.put("is_monthly", isMonthly);
        content.put("from_timestamp", fromTimestamp);
        content.put("to_timestamp", toTimestamp);
        content.put("tenant_id", tenantIndex);

        if(autoItemInfo!=null){
            for (Map.Entry<String, Object> entry : autoItemInfo.entrySet()) {
                String usageType = entry.getKey();
                Double usage = MathUtil.ObjToDouble(entry.getValue());
                content.put(usageType.toLowerCase(), usage);
            }
        }
        if(subTenantUsage!=null){
            for (Map.Entry<String, Object> entry : subTenantUsage.entrySet()) {
                String usageType = entry.getKey();
                Double usage = MathUtil.ObjToDouble(entry.getValue());
                content.put(usageType.toLowerCase(), usage);
            }
        }
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

        Map<String, Object> netUsage = getNetUsage(autoItemInfo, subTenantUsage, manualItemInfo);

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
            //get billed rates
            Map<String, Object> tpResult = queryHelper.getTableField("tariff_package_rate", "rate, gst", "id", (String) meterTypeRate.get("id"));
            if(tpResult.containsKey("error")){
                logger.severe("Failed to get tp rate: " + tpResult.get("error"));
                return Collections.singletonMap("error", "Failed to get tp rate: " + tpResult.get("error"));
            }
            Double tpRate = MathUtil.ObjToDouble(tpResult.get("rate"));
            content.put("billed_rate_"+entry.getKey().toLowerCase(), tpRate);
            content.put("billed_gst", MathUtil.ObjToDouble(tpResult.get("gst")));
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

            //check lc_status
            String lcStatus = (String) billRecs.getFirst().get("lc_status");
            if("released".equals(lcStatus)){
                logger.severe("Bill already released");
                return Collections.singletonMap("error", "Bill already released");
            }

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
            String billName = genBillingRecordName(tenantName, fromTimestamp, toTimestamp);
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

        String respStr = (String) content.get("name");
        if(billExists){
            respStr = (String) billRecs.getFirst().get("name");
        }
        return Map.of("result", respStr, "is_new", !billExists, "net_usage", netUsage);
    }

    private Map<String, Object> getNetUsage(Map<String, Object> autoItemInfo, Map<String, Object> subTenantUsage, Map<String, Object> manualItemInfo) {

        Double netUsageE = null;
        Double netUsageW = null;
        Double netUsageB = null;
        Double netUsageN = null;
        Double netUsageG = null;

        if(autoItemInfo!=null){
            if(autoItemInfo.get("billed_auto_usage_e") != null){
                netUsageE = MathUtil.ObjToDouble(autoItemInfo.get("billed_auto_usage_e"));
            }
            if(autoItemInfo.get("billed_auto_usage_w") != null){
                netUsageW = MathUtil.ObjToDouble(autoItemInfo.get("billed_auto_usage_w"));
            }
            if(autoItemInfo.get("billed_auto_usage_b") != null){
                netUsageB = MathUtil.ObjToDouble(autoItemInfo.get("billed_auto_usage_b"));
            }
            if(autoItemInfo.get("billed_auto_usage_n") != null){
                netUsageN = MathUtil.ObjToDouble(autoItemInfo.get("billed_auto_usage_n"));
            }
            if(autoItemInfo.get("billed_auto_usage_g") != null){
                netUsageG = MathUtil.ObjToDouble(autoItemInfo.get("billed_auto_usage_g"));
            }
        }
        if(subTenantUsage!=null){
            if(subTenantUsage.get("billed_sub_tenant_usage_e") != null){
                if(netUsageE == null){
                    netUsageE = 0.0;
                }
                netUsageE += MathUtil.ObjToDouble(subTenantUsage.get("billed_sub_tenant_usage_e"));
            }
            if(subTenantUsage.get("billed_sub_tenant_usage_w") != null){
                if(netUsageW == null){
                    netUsageW = 0.0;
                }
                netUsageW += MathUtil.ObjToDouble(subTenantUsage.get("billed_sub_tenant_usage_w"));
            }
            if(subTenantUsage.get("billed_sub_tenant_usage_b") != null){
                if(netUsageB == null){
                    netUsageB = 0.0;
                }
                netUsageB += MathUtil.ObjToDouble(subTenantUsage.get("billed_sub_tenant_usage_b"));
            }
            if(subTenantUsage.get("billed_sub_tenant_usage_n") != null){
                if(netUsageN == null){
                    netUsageN = 0.0;
                }
                netUsageN += MathUtil.ObjToDouble(subTenantUsage.get("billed_sub_tenant_usage_n"));
            }
            if(subTenantUsage.get("billed_sub_tenant_usage_g") != null){
                if(netUsageG == null){
                    netUsageG = 0.0;
                }
                netUsageG += MathUtil.ObjToDouble(subTenantUsage.get("billed_sub_tenant_usage_g"));
            }
        }
        if(manualItemInfo!=null){
            if(manualItemInfo.get("E") != null){
                if(netUsageE == null){
                    netUsageE = 0.0;
                }
                netUsageE += MathUtil.ObjToDouble(manualItemInfo.get("E"));
            }
            if(manualItemInfo.get("W") != null){
                if(netUsageW == null){
                    netUsageW = 0.0;
                }
                netUsageW += MathUtil.ObjToDouble(manualItemInfo.get("W"));
            }
            if(manualItemInfo.get("B") != null){
                if(netUsageB == null){
                    netUsageB = 0.0;
                }
                netUsageB += MathUtil.ObjToDouble(manualItemInfo.get("B"));
            }
            if(manualItemInfo.get("N") != null){
                if(netUsageN == null){
                    netUsageN = 0.0;
                }
                netUsageN += MathUtil.ObjToDouble(manualItemInfo.get("N"));
            }
            if(manualItemInfo.get("G") != null){
                if(netUsageG == null){
                    netUsageG = 0.0;
                }
                netUsageG += MathUtil.ObjToDouble(manualItemInfo.get("G"));
            }
        }
        Map<String, Object> netUsage = new HashMap<>();
        netUsage.put("net_usage_e", netUsageE);
        netUsage.put("net_usage_w", netUsageW);
        netUsage.put("net_usage_b", netUsageB);
        netUsage.put("net_usage_n", netUsageN);
        netUsage.put("net_usage_g", netUsageG);

        return netUsage;
    }

    public String genBillingRecordName(String tenantName, String fromTimestamp, String toTimestamp) {
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
}
