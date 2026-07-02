package org.pabuff.evs2helper.device;

import org.pabuff.oqghelper.OqgHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MeterHelper {
    @Autowired
    OqgHelper oqgHelper;

//    public ResponseEntity<Map<String, Object>> getMeterSnFromDisplayName(Map<String, String> reqMeterDisplayName){
//        if(!reqMeterDisplayName.containsKey("meter_displayname")){
//            return ResponseEntity.badRequest()
//                    .body(Collections.singletonMap("error", "meter_displayname not found"));
//        }
//        String meterDisplayName = reqMeterDisplayName.get("meter_displayname");
//
//        String meterSnQuery = "SELECT meter_sn FROM meter WHERE meter_displayname = '" + meterDisplayName + "'";
//        List<Map<String, Object>> meterSn;
//        try {
//            meterSn = oqgHelper.OqgR(meterSnQuery);
//            if(meterSn.size()==0){
//                return ResponseEntity.status(HttpStatus.NOT_FOUND)
//                        .body(Collections.singletonMap("info", "meter_displayname not found"));
//            }
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                    .body(Collections.singletonMap("error", e.getMessage()));
//        }
//        return ResponseEntity.ok(Collections.singletonMap("meter_sn", meterSn.get(0).get("meter_sn")));
//    }

    public Map<String, Object> getMeterSnFromDisplayName(String meterDisplayName){
        String meterSnQuery = "SELECT meter_sn FROM meter WHERE meter_displayname = '" + meterDisplayName + "'";
        List<Map<String, Object>> meterSn;
        try {
            meterSn = oqgHelper.OqgR(meterSnQuery);
            if(meterSn.size()==0){
                return Collections.singletonMap("info", "meter_displayname not found");
            }
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
        return Collections.singletonMap("meter_sn", meterSn.get(0).get("meter_sn"));
    }

    public Map<String, Object> getMeterDisplayNameFromSn(String meterSn){
        String meterDisplayNameQuery = "SELECT meter_displayname FROM meter WHERE meter_sn = '" + meterSn + "'";
        List<Map<String, Object>> meterDisplayName;
        try {
            meterDisplayName = oqgHelper.OqgR(meterDisplayNameQuery);
            if(meterDisplayName.size()==0){
                return Collections.singletonMap("info", "meter_displayname not found");
            }
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
        return Collections.singletonMap("meter_displayname", meterDisplayName.get(0).get("meter_displayname"));
    }
    public Map<String, Object> getMeterInfoFromSn(String meterSn){
        String meterInfoQuery = "SELECT * FROM meter WHERE meter_sn = '" + meterSn + "'";
        List<Map<String, Object>> meterInfo;
        try {
            meterInfo = oqgHelper.OqgR(meterInfoQuery);
            if(meterInfo.isEmpty()){
                return Collections.singletonMap("info", "meter_sn not found");
            }
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
        return meterInfo.getFirst();
    }

    public Map<String, Object> getMeterInfoFromDisplayName(String meterDisplayName){
        String meterInfoQuery = "SELECT * FROM meter WHERE meter_displayname = '" + meterDisplayName + "'";
        List<Map<String, Object>> meterInfo;
        try {
            meterInfo = oqgHelper.OqgR(meterInfoQuery);
            if(meterInfo.isEmpty()){
                return Collections.singletonMap("info", "meter_displayname not found");
            }
        } catch (Exception e) {
            return Collections.singletonMap("error", e.getMessage());
        }
        return meterInfo.getFirst();
    }

    public Map<String, Object> getNormalizedMeterAddress(Map<String,Object> request) {
        String source = (String) request.get("source");
        String building = (String) request.get("building");
        String block = (String) request.get("block");
        String unit = (String) request.get("unit");
        if (building == null || building.isEmpty()) {
            return Collections.singletonMap("error", "building is null or empty");
        }
        if( source == null || source.isEmpty()) {
            return Collections.singletonMap("error", "source is null or empty");
        }
        if(!source.equalsIgnoreCase("pag") && !source.equalsIgnoreCase("mms")) {
            return Collections.singletonMap("error", "source is not valid");
        }
        String normalizedBuilding = building.trim();

        String rc1aBlock = "RC1A";
        String rc1bBlock = "RC1B";
        String rc2Block = "RC2";
        String rc3aBlock = "RC3A";
        String rc3bBlock = "RC3B";
        if (normalizedBuilding.contains("College Ave West")) {
            int index = normalizedBuilding.indexOf("College Ave West");
            normalizedBuilding = normalizedBuilding.substring(0, index + "College Ave West".length());
            String value = null;
            if ("mms".equalsIgnoreCase(source)) {
                value = String.valueOf(unit.charAt(2));
            } else {
                int startIndex = building.indexOf("RC");
                if (startIndex != -1) {
                    value = String.valueOf(building.charAt(startIndex + 3));
                }
                block = normalizedBuilding.split("\\s+")[0];
            }
            if(value == null) {
                return Collections.singletonMap("error", "unit is not valid");
            }
            return switch (block) {
                case "10" -> resolveRcBlock(source,value, normalizedBuilding, rc1aBlock, rc1bBlock);
                case "12" -> Map.of("data", Map.of("building", normalizedBuilding, "block", rc2Block));
                case "28" -> resolveRcBlock(source,value, normalizedBuilding, rc3aBlock, rc3bBlock);
                default -> Collections.singletonMap("error", "block is not valid");
            };
        }
        // Normal lookup
        Map<String, Object> normalizedAddrInfo = new HashMap<>();
        for (Map<String, Object> row : meterAddrLookupTable) {
            if ("mms".equalsIgnoreCase(source)) {
                String rowBuilding = (String) row.get("mms_building_value");
                String rowBlock = (String) row.get("result_block_value");
                if (normalizedBuilding.contains(rowBuilding)) {
                    if (block == null || block.equals(rowBlock)) {
                        normalizedAddrInfo.put("building", row.get("result_building_value"));
                        normalizedAddrInfo.put("block", row.get("result_block_value"));
                        return Map.of("data", normalizedAddrInfo);
                    }
                }
            } else if ("pag".equalsIgnoreCase(source)) {
                String rowBuilding = (String) row.get("pag_building_value");
                if (normalizedBuilding.contains(rowBuilding)) {
                    normalizedAddrInfo.put("building", row.get("result_building_value"));
                    normalizedAddrInfo.put("block", row.get("result_block_value"));
                    return Map.of("data", normalizedAddrInfo);
                }
            }
        }
        return Collections.singletonMap("error", "no match found");
    }

    private Map<String, Object> resolveRcBlock(String source, String value, String building, String blockA, String blockB) {
        int rcaMinValue = 1;
        int rcaMaxValue = 2;
        int rcbMinValue = 3;
        int rcbMaxValue = 8;
        if("mms".equalsIgnoreCase(source)) {
            int intValue;
            try{
                intValue = Integer.parseInt(value);
            }catch (Exception e){
                return Collections.singletonMap("error", "unit is not valid");
            }
            if (intValue >= rcaMinValue && intValue <= rcaMaxValue) {
                    return Map.of("data", Map.of(
                                    "building", building,
                                    "block", blockA));
            }
            if (intValue >= rcbMinValue && intValue <= rcbMaxValue) {
                return Map.of("data", Map.of(
                                "building", building,
                                "block", blockB));
            }
        } else if("pag".equalsIgnoreCase(source)) {
            if("A".equalsIgnoreCase(value)){
                return Map.of("data", Map.of(
                                "building", building,
                                "block", blockA));
            }else if("B".equalsIgnoreCase(value)){
                return Map.of("data", Map.of(
                                "building", building,
                                "block", blockB));
            }else{
                return Collections.singletonMap("error", "block is not valid");
            }
        }
        return Collections.singletonMap("error", "block is not valid");
    }

    private static final List<Map<String, Object>> meterAddrLookupTable = List.of(
        Map.of(
                "result_building_value", "36 College Ave East",
                "result_block_value", "36",
                "mms_building_value", "36 College Ave East",
                "pag_building_value", "36 College Ave East"
        ),
        Map.of(
                "result_building_value", "26 College Ave East",
                "result_block_value", "26",
                "mms_building_value", "26 College Ave East",
                "pag_building_value", "26 College Ave East"
        ),
        Map.of(
                "result_building_value", "22 College Ave East",
                "result_block_value", "22",
                "mms_building_value", "22 College Ave East",
                "pag_building_value", "22 College Ave East"
        ),
        Map.of(
                "result_building_value", "8 College Ave East",
                "result_block_value", "8",
                "mms_building_value", "8 College Ave East",
                "pag_building_value", "8 College Ave East"
        ),
        Map.of(
                "result_building_value", "5 Prince George's Residence",
                "result_block_value", "5",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "5 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "6 Prince George's Residence",
                "result_block_value", "6",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "6 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "7 Prince George's Residence",
                "result_block_value", "7",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "7 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "8 Prince George's Residence",
                "result_block_value", "8",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "8 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "11 Prince George's Residence",
                "result_block_value", "11",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "11 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "12 Prince George's Residence",
                "result_block_value", "12",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "12 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "13 Prince George's Residence",
                "result_block_value", "13",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "13 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "14 Prince George's Residence",
                "result_block_value", "14",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "14 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "17 Prince George's Residence",
                "result_block_value", "17",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "17 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "18 Prince George's Residence",
                "result_block_value", "18",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "18 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "19 Prince George's Residence",
                "result_block_value", "19",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "19 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "21 Prince George's Residence",
                "result_block_value", "21",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "21 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "23 Prince George's Residence",
                "result_block_value", "23",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "23 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "25 Prince George's Residence",
                "result_block_value", "25",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "25 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "26 Prince George's Residence",
                "result_block_value", "26",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "26 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "28 Prince George's Residence",
                "result_block_value", "28",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "28 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "30 Prince George's Residence",
                "result_block_value", "30",
                "mms_building_value", "Prince George's Residence",
                "pag_building_value", "30 Prince George's Residence"
        ),
        Map.of(
                "result_building_value", "Maple Residence",
                "result_block_value", "Maple Residences",
                "mms_building_value", "Maple Residences",
                "pag_building_value", "Maple Residence"
        ),
        Map.of(
                "result_building_value", "Valour House Building A",
                "result_block_value", "A",
                "mms_building_value", "Valour House Building A",
                "pag_building_value", "Valour House Building A"
        ),
        Map.of(
                "result_building_value", "25E Lower Kent Ridge Rd",
                "result_block_value", "E",
                "mms_building_value", "25E Lower Kent Ridge Rd",
                "pag_building_value", "25E Lower Kent Ridge Rd"
        ),
        Map.of(
                "result_building_value", "Block A 10 Heng Mui Keng Ter",
                "result_block_value", "A",
                "mms_building_value", "Block A 10 Heng Mui Keng Ter",
                "pag_building_value", "Block A 10 Heng Mui Keng Ter"
        ),
        Map.of(
                "result_building_value", "Block B 10 Heng Mui Keng Ter",
                "result_block_value", "B",
                "mms_building_value", "Block B 10 Heng Mui Keng Ter",
                "pag_building_value", "Block B 10 Heng Mui Keng Ter"
        ),
        Map.of(
                "result_building_value", "Block A 20 Heng Mui Keng Ter",
                "result_block_value", "A",
                "mms_building_value", "Block A 20 Heng Mui Keng Ter",
                "pag_building_value", "Block A 20 Heng Mui Keng Ter"
        ),
        Map.of(
                "result_building_value", "Block B 20 Heng Mui Keng Ter",
                "result_block_value", "B",
                "mms_building_value", "Block B 20 Heng Mui Keng Ter",
                "pag_building_value", "Block B 20 Heng Mui Keng Ter"
        ),
        Map.of(
                "result_building_value", "Block B 1A Kent Ridge Rd",
                "result_block_value", "B",
                "mms_building_value", "Block B 1A Kent Ridge Rd",
                "pag_building_value", "Block B 1A Kent Ridge Rd"
        ),
        Map.of(
                "result_building_value", "Block C 1A Kent Ridge Rd",
                "result_block_value", "C",
                "mms_building_value", "Block C 1A Kent Ridge Rd",
                "pag_building_value", "Block C 1A Kent Ridge Rd"
        ),
        Map.of(
                "result_building_value", "Block D 1A Kent Ridge Rd",
                "result_block_value", "D",
                "mms_building_value", "Block D 1A Kent Ridge Rd",
                "pag_building_value", "Block D 1A Kent Ridge Rd"
        ),
        Map.of(
                "result_building_value", "Block A 10 Kent Ridge Dr",
                "result_block_value", "A",
                "mms_building_value", "Block A 10 Kent Ridge Dr",
                "pag_building_value", "Block A 10 Kent Ridge Dr"
        ),
        Map.of(
                "result_building_value", "Block A 12 Kent Ridge Dr",
                "result_block_value", "A",
                "mms_building_value", "Block A 12 Kent Ridge Dr",
                "pag_building_value", "Block A 12 Kent Ridge Dr"
        )
    );
}
