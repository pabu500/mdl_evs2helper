package com.pabu5h.evs2.evs2helper.fleet_health;

import com.pabu5h.evs2.evs2helper.locale.LocalHelper;
import com.pabu5h.evs2.evs2helper.scope.ScopeHelper;
import com.pabu5h.evs2.oqghelper.OqgHelper;
import com.pabu5h.evs2.oqghelper.QueryHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class FleetStatProcessor {
    Logger logger = Logger.getLogger(FleetStatProcessor.class.getName());

    @Autowired
    private LocalHelper localHelper;
    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private ScopeHelper scopeHelper;
    @Autowired
    private QueryHelper queryHelper;

    public Map<String, Object> getSiteStatHealth(Map<String, Object> request) {
        String projectScope = (String) request.get("project_scope");
        List<String> siteTags = (List<String>) request.get("site_tags");
        String selectedSiteTag = (String) request.get("site_tag");
        boolean isSingleSite = selectedSiteTag != null && !selectedSiteTag.isEmpty();

        Map<String, Object> scope = scopeHelper.getItemTypeConfig(projectScope, null);
        if (scope.containsKey("error")) {
            return scope;
        }
        String targetTableName = (String) scope.get("targetTableName");
//        String itemIdColSel = (String) scope.get("itemIdColSel");
        String itemLocBuildingColName = (String) scope.get("itemLocBuildingColName");
        String itemLocBlockColName = (String) scope.get("itemLocBlockColName");

        String lcStatusConstraint = " and (lc_status != 'dc' OR lc_status is null) ";

        String additionalConstraint = " and " + itemLocBuildingColName + " is not null "; //and mms_building not like '%NUS %' and mms_building not like '%NTU %'";
        if(projectScope.toLowerCase().contains("ems_cw_nus")){
            additionalConstraint = "";
        }

        LocalDateTime localNow = localHelper.getLocalNow();
        Duration lastReadingTooLong = Duration.ofHours(48);
        String lastReadingHealthFilter = " last_reading_timestamp < '" + localNow.minus(lastReadingTooLong) + "'";

        double balTooLarge = 150;
        double balTooSmall = -10;
        String balHealthFilter = " (last_credit_bal > " + balTooLarge + " or last_credit_bal < " + balTooSmall + ")";
        if(projectScope.toLowerCase().contains("ems_cw_nus")){
            balHealthFilter = "";
        }

//        String healthFilter = lastReadingHealthFilter + " or " + balHealthFilter;
//        if(projectScope.toLowerCase().contains("ems_cw_nus")){
//            healthFilter = lastReadingHealthFilter;
//        }

        List<Map<String, Object>> siteStats = new ArrayList<>();
        if(isSingleSite){
            Map<String, Object> result = getSingleSiteStat(
                    projectScope, selectedSiteTag,
                    targetTableName,
                    itemLocBuildingColName, itemLocBlockColName,
                    lcStatusConstraint,
                    additionalConstraint,
                    lastReadingHealthFilter, balHealthFilter
            );
            if(result.containsKey("error")){
                return result;
            }
            siteStats = (List<Map<String, Object>>) result.get("result");
        }else {
            Map<String, Object> result = getFullSiteStat(
                    projectScope, siteTags,
                    targetTableName,
                    itemLocBuildingColName, itemLocBlockColName,
                    lcStatusConstraint,
                    additionalConstraint,
                    lastReadingHealthFilter, balHealthFilter
            );
            if(result.containsKey("error")){
                return result;
            }
            siteStats = (List<Map<String, Object>>) result.get("result");
        }

        return Map.of("result", siteStats);
    }

    private Map<String, Object> getFullSiteStat(
            String projectScope, List<String> siteTags,
            String targetTableName,
            String itemLocBuildingColName, String itemLocBlockColName,
            String lcStatusConstraint,
            String additionalConstraint,
            String lastReadingHealthFilter, String balHealthFilter){

        List<Map<String, Object>> siteStats = new ArrayList<>();
        for (String siteTag : siteTags) {
            Map<String, Object> siteStat = new HashMap<>();

            siteTag = siteTag.toLowerCase();
//                if (projectScope.toLowerCase().contains("ems_cw_nus")) {
//                    if (!siteTag.startsWith("site_")) {
//                        siteTag = "site_" + siteTag;
//                    }
//                }

            siteStat.put("site_tag", siteTag);

            String sqlAll = "select count(*) as count from " + targetTableName
                    + " where site_tag = '" + siteTag + "'"
                    + lcStatusConstraint
                    + additionalConstraint;

            List<Map<String, Object>> respAll;
            try {
                respAll = oqgHelper.OqgR2(sqlAll, true);
            } catch (Exception e) {
                logger.severe(e.getMessage());
                return Map.of("error", e.getMessage());
            }
            siteStat.put("total", respAll.getFirst().get("count"));

            String sqlLRT = "select count(*) as count from " + targetTableName
                    + " where site_tag = '" + siteTag + "'"
                    + " and " + lastReadingHealthFilter
                    + lcStatusConstraint
                    + additionalConstraint;
            List<Map<String, Object>> respLRT;
            try {
                respLRT = oqgHelper.OqgR2(sqlLRT, true);
            } catch (Exception e) {
                logger.severe(e.getMessage());
                return Map.of("error", e.getMessage());
            }
            siteStat.put("last_reading_too_long", respLRT.getFirst().get("count"));

            boolean checkBalHealth = !balHealthFilter.isEmpty()
                    && !siteTag.toLowerCase().contains("pgpr");

            if (checkBalHealth) {
                String sqlBal = "select count(*) as count from " + targetTableName
                        + " where site_tag = '" + siteTag + "'"
                        + " and " + balHealthFilter
                        + " and meter_sn in (select meter_sn from cpc_policy) "
                        + lcStatusConstraint
                        + additionalConstraint;
                List<Map<String, Object>> respBal;
                try {
                    respBal = oqgHelper.OqgR2(sqlBal, true);
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                    return Map.of("error", e.getMessage());
                }
                siteStat.put("credit_bal_range", respBal.getFirst().get("count"));
            }
            siteStats.add(siteStat);
        }
        return Map.of("result", siteStats);
    }

    private Map<String, Object> getSingleSiteStat(
            String projectScope, String selectedSiteTag,
            String targetTableName,
            String itemLocBuildingColName, String itemLocBlockColName,
            String lcStatusConstraint,
            String additionalConstraint,
            String lastReadingHealthFilter, String balHealthFilter) {

        List<Map<String, Object>> siteStats = new ArrayList<>();
        selectedSiteTag = selectedSiteTag.toLowerCase();

        Map<String, Object> result = queryHelper.getScopeBuildings(projectScope, selectedSiteTag);
        if(result.containsKey("error")){
            return result;
        }
        List<String> buildings = (List<String>) result.get("building_list");
        if(buildings.isEmpty()){
            return Map.of("error", "No buildings found for site " + selectedSiteTag);
        }
        for (String buildingName : buildings) {
            if (buildingName == null) {
                continue;
            }
            if (buildingName.isEmpty()) {
                continue;
            }
            if(buildingName.equals("TBA")){
                continue;
            }
//                logger.info("Building: " + buildingName);

            Map<String, Object> blkResult = new HashMap<>();
            List<String> blks = new ArrayList<>();
            if(!itemLocBlockColName.isEmpty()){
                blkResult = queryHelper.getMmsBuildingBlocks(buildingName, null, selectedSiteTag);
                if (blkResult.containsKey("error")) {
                    return blkResult;
                }
                if (blkResult.containsKey("info")) {
                    continue;
                }
                blks = (List<String>) blkResult.get("block_list");
            }
            if(blks.isEmpty()){
                blks.add("");
            }

            for (String blk : blks) {
                Map<String, Object> siteStat = new HashMap<>();
                siteStat.put("site_tag", selectedSiteTag);

                String buildingLabel = buildingName;
                buildingLabel = buildingLabel.replace("Maple Residences", "MR");
                buildingLabel = buildingLabel.replace("Prince George's Residence", "PGPR");
                buildingLabel = buildingLabel.replace("Ridge View Residence", "RVRC");
                buildingLabel = buildingLabel.replace("College Ave West", "CAW");
                buildingLabel = buildingLabel.replace("22 College Ave East", "Cinnamon");
                buildingLabel = buildingLabel.replace("26 College Ave East", "Tembusu");
                buildingLabel = buildingLabel.replace("36 College Ave East", "North Tower");
                buildingLabel = buildingLabel.replace("38 College Ave East", "South Tower");
                //put last to avoid replacing "26 College Ave East" or "36 College Ave East" to "2RC4" or "3RC4"
                buildingLabel = buildingLabel.replace("6 College Ave East", "RC4");
                buildingLabel = buildingLabel.replace("8 College Ave East", "CAPT");
                String blockId = buildingLabel;
                if (!blk.isEmpty()) {
                    blockId += "-" + blk;
                }
                siteStat.put("block_id", blockId);
                siteStat.put("building_name", buildingName);
                siteStat.put("building_label", buildingLabel);
                siteStat.put("block", blk);

                String buildingNameSql = buildingName.replace("'", "''");

                String blockSel = "";
                if (!itemLocBlockColName.isEmpty()) {
                    blockSel = " and " + itemLocBlockColName + " = '" + blk + "'";
                }

                String sqlAll = "select count(*) as count from " + targetTableName
                        + " where site_tag = '" + selectedSiteTag + "'"
                        + " and " + itemLocBuildingColName + " = '" + buildingNameSql + "'"
                        + blockSel
                        + lcStatusConstraint
                        + additionalConstraint;
                List<Map<String, Object>> respAll;
                try {
                    respAll = oqgHelper.OqgR2(sqlAll, true);
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                    return Map.of("error", e.getMessage());
                }
                siteStat.put("total", respAll.getFirst().get("count"));

                String sqlLRT = "select count(*) as count from " + targetTableName
                        + " where site_tag = '" + selectedSiteTag + "'"
                        + " and " + itemLocBuildingColName + " = '" + buildingNameSql + "'"
                        + blockSel
                        + " and " + lastReadingHealthFilter
                        + lcStatusConstraint
                        + additionalConstraint;
                List<Map<String, Object>> respLRT;
                try {
                    respLRT = oqgHelper.OqgR2(sqlLRT, true);
                } catch (Exception e) {
                    logger.severe(e.getMessage());
                    return Map.of("error", e.getMessage());
                }
                siteStat.put("last_reading_too_long", respLRT.getFirst().get("count"));

                boolean checkBalHealth = !balHealthFilter.isEmpty()
                        && !selectedSiteTag.toLowerCase().contains("pgpr");

                if (checkBalHealth) {
                    String sqlBal = "select count(*) as count from " + targetTableName
                            + " where site_tag = '" + selectedSiteTag + "'"
                            + " and " + itemLocBuildingColName + " = '" + buildingNameSql + "'"
                            + blockSel
                            + " and " + balHealthFilter
                            + lcStatusConstraint
                            + additionalConstraint;
                    List<Map<String, Object>> respBal;
                    try {
                        respBal = oqgHelper.OqgR2(sqlBal, true);
                    } catch (Exception e) {
                        logger.severe(e.getMessage());
                        return Map.of("error", e.getMessage());
                    }
                    siteStat.put("credit_bal_range", respBal.getFirst().get("count"));
                }
                siteStats.add(siteStat);
            }
        }
        return Map.of("result", siteStats);
    }

    public Map<String, Object> getBuildingBlockReport(Map<String, Object> request) {
        String projectScope = (String) request.get("project_scope");
        String building = (String) request.get("building");
        String buildingNameSqlSafe = building.replace("'", "''");
        String block = (String) request.get("block");

        Map<String, Object> scope = scopeHelper.getItemTypeConfig(projectScope, null);
        if (scope.containsKey("error")) {
            return scope;
        }
        String targetTableName = (String) scope.get("targetTableName");
        String itemIdColSel = (String) scope.get("itemIdColSel");
        String itemLocColSel = (String) scope.get("itemLocColSel");
        String itemLocBuildingColName = (String) scope.get("itemLocBuildingColName");
        String itemLocBlockColName = (String) scope.get("itemLocBlockColName");
        String blockSel = "";
        if (!itemLocBlockColName.isEmpty()) {
            blockSel = " and " + itemLocBlockColName + " = '" + block + "'";
        }

        LocalDateTime localNow = localHelper.getLocalNow();
        Duration lastReadingTooLong = Duration.ofHours(48);
        String lastReadingHealthFilter = " last_reading_timestamp < '" + localNow.minus(lastReadingTooLong) + "'";

        double balTooLarge = 150;
        double balTooSmall = -10;
        String balHealthFilter = " (last_credit_bal > " + balTooLarge + " or last_credit_bal < " + balTooSmall + ")";
        if(projectScope.toLowerCase().contains("ems_cw_nus")){
            balHealthFilter = "";
        }

        String lcStatusConstraint = " and (lc_status != 'dc' OR lc_status is null) ";

        Map<String, Object> report = new HashMap<>();

        String sqlReading = "select last_reading_timestamp, " + itemIdColSel +","+ itemLocColSel + " from " + targetTableName
                + " where " + itemLocBuildingColName + " = '" + buildingNameSqlSafe + "'"
                + blockSel
                + " and " + lastReadingHealthFilter
                + lcStatusConstraint
                + " order by last_reading_timestamp desc";
        List<Map<String, Object>> respReading;
        try {
            respReading = oqgHelper.OqgR2(sqlReading, true);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
        report.put("last_reading_too_long", respReading);

        boolean checkBalHealth = !balHealthFilter.isEmpty();
        if (checkBalHealth) {
            String sqlBal = "select last_credit_bal, last_reading_timestamp, "
                    + itemIdColSel +","+ itemLocColSel + " from " + targetTableName
                    + " where "+ itemLocBuildingColName + " = '" + buildingNameSqlSafe + "'"
                    + blockSel
                    + " and " + balHealthFilter
                    + lcStatusConstraint
                    + " and meter_sn in (select meter_sn from cpc_policy)"
                    + " order by last_credit_bal asc";
            List<Map<String, Object>> respBal;
            try {
                respBal = oqgHelper.OqgR2(sqlBal, true);
            } catch (Exception e) {
                logger.severe(e.getMessage());
                return Map.of("error", e.getMessage());
            }
            report.put("credit_bal_range", respBal);
        }
        return Map.of("result", report);
    }
}
