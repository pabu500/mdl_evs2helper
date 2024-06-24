package org.pabuff.evs2helper.scope;

import org.pabuff.dto.ItemIdTypeEnum;
import org.pabuff.dto.ItemTypeEnum;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;

@Service
public class ScopeHelper {
    Logger logger = Logger.getLogger(ScopeHelper.class.getName());

    public Map<String, String> getRequestScope(Map<String, Object> request){
        String projectScope = (String) request.get("project_scope");
        String siteScope = (String) request.get("site_scope");
        Map<String, String> scope = new HashMap<>();
        if(siteScope != null && (!siteScope.isEmpty())){
            //site_scope for app, site_tag for db
            scope.put("site_tag", siteScope.toLowerCase());
        }else if(projectScope != null && (!projectScope.isEmpty())){
            //project_scope for app, scope_str for db
            scope.put("scope_str", projectScope.toLowerCase());

            if(projectScope.equalsIgnoreCase("sg_all")){
                scope = null;
            }
        }else{
            return Collections.singletonMap("error", "Invalid scope");
        }
        return scope;
    }

    public Map<String, Object> getItemTypeConfig(String projectScope, String itemIdTypeStr){
        ItemIdTypeEnum itemIdType = (itemIdTypeStr == null || itemIdTypeStr.isEmpty()) ? null : ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
        String targetTableName = "meter";
        String targetReadingIndexColName = "id";
        String targetReadingTableName = "meter_reading";
        String targetReadingIdColName = "meter_sn";
        String targetGroupTableName = "meter_group";
        String targetGroupTargetTableName = "meter_group_meter";
        String tenantTableName = "tenant";
        String tenantTargetGroupTableName = "tenant_meter_group";
        String itemIdColName = "meter_sn";
        String itemSnColName = "meter_sn";
        String itemNameColName = "meter_displayname";
        String timeKey = "kwh_timestamp";
        String valKey = "kwh_total";
        String itemAltNameColName = "alt_name";
        String panelTagColName = "panel_tag";
        String itemIdColSel = "meter_sn,meter_displayname";
        String itemLocColSel = "mms_building,mms_block,mms_level,mms_unit";
        String itemLocBuildingColName = "mms_building";
        String itemLocBlockColName = "mms_block";
        ItemTypeEnum itemType = ItemTypeEnum.METER;
        Function<String, String> validator = null;
        if (projectScope.toLowerCase().contains("ems_smrt")) {
            itemType = ItemTypeEnum.METER_3P;
            targetReadingTableName = "meter_reading_3p";
            targetTableName = "meter_3p";
            itemIdColName = "meter_id";
            itemSnColName = "meter_sn";
            itemNameColName = "meter_id";
            itemIdColSel = "meter_id,meter_sn,panel_tag";
            itemLocColSel = "panel_tag";
            timeKey = "dt";
            valKey = "a_imp";

            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.NAME;
            }
            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "meter_id";
            }
        } else if (projectScope.toLowerCase().contains("ems_cw_nus")) {
            itemType = ItemTypeEnum.METER_IWOW;
            targetReadingTableName = "meter_reading_iwow";
            targetTableName = "meter_iwow";
            targetGroupTableName = "meter_group";
            targetGroupTargetTableName = "meter_group_meter_iwow";
            tenantTableName = "tenant";
            tenantTargetGroupTableName = "tenant_meter_group_iwow";
            itemIdColName = "item_name";
            itemSnColName = "item_sn";
            itemNameColName = "item_name";
            itemAltNameColName = "alt_name";
            itemIdColSel = "item_sn,item_name,alt_name";
            itemLocColSel = "loc_building,loc_level";
            itemLocBuildingColName = "loc_building";
            itemLocBlockColName = "";
            timeKey = "dt";
            valKey = "val";
            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.NAME;
            }

            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "item_name";
            }
        } else if (projectScope.toLowerCase().contains("ems_zsp")){
            itemType = ItemTypeEnum.METER_ZSP;
            targetTableName = "recorder";
            targetReadingIndexColName = "egyid";
            targetReadingTableName = "energy_etc";
            targetGroupTableName = "recgroup";
            tenantTableName = "tenant";
            tenantTargetGroupTableName = "tenantgroup";
            itemIdColName = "recid";
            targetReadingIdColName = "egyinstkey";
            itemNameColName = "recdisplayname";
            timeKey = "timestamp";
            valKey = "kwhtot";
            itemLocBuildingColName = "buildingname";

        }else if (projectScope.toLowerCase().contains("ems_mbfc")){
            itemType = ItemTypeEnum.METER_MBFC;
            targetTableName = "recorder";
            targetReadingIndexColName = "egyid";
            targetReadingTableName = "energy_etc";
            targetGroupTableName = "recgroup";
            tenantTableName = "customer";
            tenantTargetGroupTableName = "custgroup";
            itemIdColName = "recid";
            targetReadingIdColName = "egyinstkey";
            itemNameColName = "recdisplayname";
            timeKey = "timestamp";
            valKey = "kwhtot";
            itemLocBuildingColName = "buildingname";
        }
        else {
            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.SN;
            }

            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "meter_displayname";
                validator = this::validateNameMms;
            }else if (itemIdType == ItemIdTypeEnum.SN) {
                itemIdColName = "meter_sn";
                validator = this::validateSnMms;
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("itemTypeEnum", itemType.toString());
        result.put("targetReadingTableName", targetReadingTableName);
        result.put("targetReadingIndexColName", targetReadingIndexColName);
        result.put("targetReadingIdColName", targetReadingIdColName);
        result.put("targetTableName", targetTableName);
        result.put("targetGroupTableName", targetGroupTableName);
        result.put("targetGroupTargetTableName", targetGroupTargetTableName);
        result.put("tenantTableName", tenantTableName);
        result.put("tenantTargetGroupTableName", tenantTargetGroupTableName);
        result.put("itemIdColName", itemIdColName);
        result.put("itemSnColName", itemSnColName);
        result.put("itemNameColName", itemNameColName);
        result.put("itemAltNameColName", itemAltNameColName);
        result.put("panelTagColName", panelTagColName);
        result.put("itemIdColSel", itemIdColSel);
        result.put("itemLocColSel", itemLocColSel);
        result.put("itemLocBuildingColName", itemLocBuildingColName);
        result.put("itemLocBlockColName", itemLocBlockColName);
        result.put("timeKey", timeKey);
        result.put("valKey", valKey);
        result.put("validator", validator);

        return result;
    }
    public Map<String, Object> getItemTypeConfig2(String projectScope, String itemIdTypeStr){
        ItemIdTypeEnum itemIdType = (itemIdTypeStr == null || itemIdTypeStr.isEmpty()) ? null : ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
        String itemTableName = "meter";
        String itemReadingTableName = "meter_reading";
        String itemReadingIndexColName = "id";
        String itemReadingIdColName = "meter_sn";
        String itemUsageTableName = "meter_tariff";
        String itemGroupTableName = "meter_group";
        String itemGroupTargetTableName = "meter_group_meter";
        String tenantTableName = "tenant";
        String tenantTargetGroupTableName = "tenant_meter_group";
        String itemIdColName = "meter_sn";
        String itemSnColName = "meter_sn";
        String itemNameColName = "meter_displayname";
        String timeKey = "kwh_timestamp";
        String valKey = "kwh_total";
        String valDiffKey = "kwh_diff";
        String valDiffTimeKey = "tariff_timestamp";
        String itemAltNameColName = "alt_name";
        String panelTagColName = "panel_tag";
        String itemIdColSel = "meter_sn,meter_displayname";
        String itemLocColSel = "mms_building,mms_block,mms_level,mms_unit";
        String itemLocBuildingColName = "mms_building";
        String itemLocBlockColName = "mms_block";
        ItemTypeEnum itemType = ItemTypeEnum.METER;
        Function<String, String> validator = null;
        if (projectScope.toLowerCase().contains("ems_smrt")) {
            itemType = ItemTypeEnum.METER_3P;
            itemReadingTableName = "meter_reading_3p";
            itemReadingIdColName = "meter_id";
            itemTableName = "meter_3p";
            itemIdColName = "meter_id";
            itemSnColName = "meter_sn";
            itemNameColName = "meter_id";
            itemIdColSel = "meter_id,meter_sn,panel_tag";
            itemLocColSel = "panel_tag";
            timeKey = "dt";
            valKey = "a_imp";

            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.NAME;
            }
            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "meter_id";
            }
        } else if (projectScope.toLowerCase().contains("ems_cw_nus")) {
            itemType = ItemTypeEnum.METER_IWOW;
            itemReadingTableName = "meter_reading_iwow";
            itemReadingIdColName = "item_name";
            itemUsageTableName = "meter_reading_iwow";
            itemTableName = "meter_iwow";
            itemGroupTableName = "meter_group";
            itemGroupTargetTableName = "meter_group_meter_iwow";
            tenantTableName = "tenant";
            tenantTargetGroupTableName = "tenant_meter_group_iwow";
            itemIdColName = "item_name";
            itemSnColName = "item_sn";
            itemNameColName = "item_name";
            itemAltNameColName = "alt_name";
            itemIdColSel = "item_sn,item_name,alt_name";
            itemLocColSel = "loc_building,loc_level";
            itemLocBuildingColName = "loc_building";
            itemLocBlockColName = "";
            timeKey = "dt";
            valKey = "val";
            valDiffKey = "val_diff";
            valDiffTimeKey = "dt";
            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.NAME;
            }

            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "item_name";
            }
        }else if (projectScope.toLowerCase().contains("ems_zsp")) {
            itemType = ItemTypeEnum.METER_ZSP;
            itemTableName = "recorder";
            itemReadingTableName = "energy_etc";
            itemReadingIndexColName = "egyid";
            itemReadingIdColName = "egyinstkey";
            itemGroupTableName = "recgroup";
            tenantTableName = "tenant";
            tenantTargetGroupTableName = "tenantgroup";
            itemIdColName = "recid";
//            itemNameColName = "recdisplayname";
            itemNameColName = "recid";
            itemSnColName = "recid";
            timeKey = "timestamp";
            valKey = "kwhtot";
            itemLocBuildingColName = "buildingname";
        }else if (projectScope.toLowerCase().contains("ems_mbfc")) {
            itemType = ItemTypeEnum.METER_MBFC;
            itemTableName = "recorder";
            itemReadingTableName = "energy_etc";
            itemReadingIndexColName = "egyid";
            itemReadingIdColName = "egyinstkey";
            itemGroupTableName = "recgroup";
            tenantTableName = "customer";
            tenantTargetGroupTableName = "custgroup";
            itemIdColName = "recid";
//            itemNameColName = "recdisplayname";
            itemNameColName = "recid";
            itemSnColName = "recid";
            timeKey = "timestamp";
            valKey = "kwhtot";
            itemLocBuildingColName = "buildingname";
        }else {
            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.SN;
            }

            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "meter_displayname";
                validator = this::validateNameMms;
            }else if (itemIdType == ItemIdTypeEnum.SN) {
                itemIdColName = "meter_sn";
                validator = this::validateSnMms;
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("itemTypeEnum", itemType.toString());
        result.put("itemReadingTableName", itemReadingTableName);
        result.put("itemReadingIndexColName", itemReadingIndexColName);
        result.put("itemReadingIdColName", itemReadingIdColName);
        result.put("itemUsageTableName", itemUsageTableName);
        result.put("itemTableName", itemTableName);
        result.put("itemGroupTableName", itemGroupTableName);
        result.put("itemGroupTargetTableName", itemGroupTargetTableName);
        result.put("tenantTableName", tenantTableName);
        result.put("tenantTargetGroupTableName", tenantTargetGroupTableName);
        result.put("itemIdColName", itemIdColName);
        result.put("itemSnColName", itemSnColName);
        result.put("itemNameColName", itemNameColName);
        result.put("itemAltNameColName", itemAltNameColName);
        result.put("panelTagColName", panelTagColName);
        result.put("itemIdColSel", itemIdColSel);
        result.put("itemLocColSel", itemLocColSel);
        result.put("itemLocBuildingColName", itemLocBuildingColName);
        result.put("itemLocBlockColName", itemLocBlockColName);
        result.put("timeKey", timeKey);
        result.put("valKey", valKey);
        result.put("valDiffKey", valDiffKey);
        result.put("valDiffTimeKey", valDiffTimeKey);
        result.put("validator", validator);

        return result;
    }

    public String validateNameMms(String input) {
        if(input == null || input.isBlank()) {
            return "displayname is blank";
        }
        if(input.length() != 8) {
            return "displayname length must be 8";
        }
        if(!input.matches("[0-9]+")) {
            return "displayname must be numeric";
        }
        //must start with 1, 2, or 3
        if(!input.startsWith("1") && !input.startsWith("2") && !input.startsWith("3")) {
            return "displayname must start with 1, 2, or 3";
        }
        return null;
    }
    public String validateSnMms(String input) {
        if(input == null || input.isBlank()) {
            return "sn is blank";
        }
        if(input.length() != 12) {
            return "sn length must be 12";
        }
        if(!input.matches("[0-9]+")) {
            return "sn must be numeric";
        }
        //must start with 20 or 0 (detached sn)
        if(!input.startsWith("20") && !input.startsWith("0")) {
            return "sn must start with 20 or 0";
        }
        return null;
    }
//
//    private String validateDefault(String input) {
//        // Default validation logic
//        return "Validated Default: " + input;
//    }
//
//    public String validator(String projectScope, String itemIdTypeStr, String input) {
//        ItemIdTypeEnum itemIdType = ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
//        Function<String, String> validator = validationMap.getOrDefault(itemIdType, this::validateDefault);
//        return validator.apply(input);
//    }
}
