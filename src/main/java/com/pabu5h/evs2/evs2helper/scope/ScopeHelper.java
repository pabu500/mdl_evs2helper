package com.pabu5h.evs2.evs2helper.scope;

import com.pabu5h.evs2.dto.ItemIdTypeEnum;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;

@Service
public class ScopeHelper {
    Logger logger = Logger.getLogger(ScopeHelper.class.getName());

    public Map<String, Object> getItemTypeConfig(String projectScope, String itemIdTypeStr){
        ItemIdTypeEnum itemIdType = itemIdTypeStr.isEmpty()? null : ItemIdTypeEnum.valueOf(itemIdTypeStr.toUpperCase());
        String targetTableName = "meter";
        String targetReadingTableName = "meter_reading";
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
        Function<String, String> validator = null;
        if (projectScope.toLowerCase().contains("ems_smrt")) {
            targetReadingTableName = "meter_reading_3p";
            targetTableName = "meter_3p";
            itemIdColName = "meter_id";
            itemSnColName = "meter_sn";
            itemNameColName = "meter_id";
            itemIdColSel = "meter_id,meter_sn,panel_tag";
            itemLocColSel = "penal_tag";
            timeKey = "dt";
            valKey = "a_imp";

            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.NAME;
            }
            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "meter_id";
            }
        } else if (projectScope.toLowerCase().contains("ems_cw_nus")) {
            targetReadingTableName = "meter_reading_iwow";
            targetTableName = "meter_iwow";
            targetGroupTableName = "meter_group";
            targetGroupTargetTableName = "meter_group_meter_iwow";
            tenantTableName = "tenant";
            tenantTargetGroupTableName = "tenant_meter_group_iwow";
            itemIdColName = "item_sn";
            itemSnColName = "item_sn";
            itemNameColName = "item_name";
            itemAltNameColName = "alt_name";
            itemIdColSel = "item_sn,item_name,alt_name";
            itemLocColSel = "loc_building,loc_level";
            timeKey = "dt";
            valKey = "val";
            if(itemIdType == null){
                itemIdType = ItemIdTypeEnum.NAME;
            }

            if (itemIdType == ItemIdTypeEnum.NAME) {
                itemIdColName = "item_name";
            }
        } else {
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
        result.put("targetReadingTableName", targetReadingTableName);
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
        result.put("timeKey", timeKey);
        result.put("valKey", valKey);
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
        //must start with 20
        if(!input.startsWith("20")) {
            return "sn must start with 20";
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
