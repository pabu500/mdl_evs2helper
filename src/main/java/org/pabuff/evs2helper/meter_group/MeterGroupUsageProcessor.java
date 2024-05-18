package org.pabuff.evs2helper.meter_group;

import org.pabuff.evs2helper.meter_usage.MeterUsageProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class MeterGroupUsageProcessor {
    Logger logger = Logger.getLogger(MeterGroupUsageProcessor.class.getName());

    @Autowired
    private MeterUsageProcessor meterUsageProcessor;

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
