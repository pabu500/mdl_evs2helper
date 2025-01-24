package org.pabuff.evs2helper.meter_reading;

import org.pabuff.oqghelper.DevOqgHelper;
import org.pabuff.oqghelper.OqgHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class MeterReadingProcessor {

    Logger logger = Logger.getLogger(MeterReadingProcessor.class.getName());

    @Autowired
    private OqgHelper oqgHelper;
    @Autowired
    private DevOqgHelper devOqgHelper;

    public Map<String, Object> getReadingListWithPeriod(Map<String, Object> req){
        logger.info("getMeterReadingListWithPeriod");
        Map<String, Object> result = new HashMap<>();

        if(req == null){
            return Map.of("error", "request is null");
        }

        if(req.get("item_reading_id_col_name") == null || req.get("item_reading_table_name") == null){
            logger.warning("no reading column or table name found");
            return Map.of("error", "item_reading_id_col_name, item_reading_table_name are required");
        }

        if(req.get("item_reading_id") == null){
            if(req.get("item_id") == null || req.get("item_id_col_name") == null || req.get("item_table_name") == null || req.get("item_join_col_name") == null){
                logger.warning("no item id, column, table, or join column name found");
                return Map.of("error", "no item id, column, table, or join column name found");
            }
        }

        if(req.get("from_timestamp") == null || req.get("to_timestamp") == null){
            logger.warning("from_timestamp, to_timestamp are required");
            return Map.of("error", "from_timestamp, to_timestamp are required");
        }

        if(req.get("val_key") == null || req.get("time_key") == null){
            logger.warning("no val_key or time_key found");
            return Map.of("error", "val_key, time_key are required");
        }

        String valKey = (String) req.get("val_key");
        String timeKey = (String) req.get("time_key");
        String itemReadingId = (String) req.get("item_reading_id");
        String itemReadingIdColName = (String) req.get("item_reading_id_col_name");
        String itemReadingTableName = (String) req.get("item_reading_table_name");
        String itemId = (String) req.get("item_id");
        String itemIdColName = (String) req.get("item_id_col_name");
        String itemTableName = (String) req.get("item_table_name");
        String itemJoinColName = (String) req.get("item_join_col_name");
        String fromTimestamp = (String) req.get("from_timestamp");
        String toTimestamp = (String) req.get("to_timestamp");
        String query = "";
        boolean devDb = (boolean) req.get("dev_db");

        if(itemReadingId != null){
            query = "SELECT " + timeKey + " AS kwh_timestamp, " + valKey + " AS kwh_total FROM " + itemReadingTableName + " WHERE " + itemReadingIdColName
                    + " = '" + itemReadingId + "' AND " + timeKey + " >= '" + fromTimestamp + "' AND " + timeKey + " < '" + toTimestamp
                    + "' GROUP BY " + timeKey + ", " + valKey + " HAVING COUNT(*) = 1 ORDER BY " + timeKey + " ASC;";
        }else if(itemId != null){
            query = "SELECT " + timeKey + " AS kwh_timestamp, " + valKey + " AS kwh_total FROM " + itemReadingTableName + " r JOIN " + itemTableName + " i ON r." + itemReadingIdColName + " = i." + itemJoinColName
                    + " WHERE i." + itemIdColName + " = '" + itemId + "' AND r."
                    + timeKey + " >= '" + fromTimestamp + "' AND r."
                    + timeKey + " < '" + toTimestamp
                    + "' GROUP BY r." + timeKey + ", r." + valKey
                    + " HAVING COUNT(*) = 1 ORDER BY r." + timeKey + " ASC;";
        }

        List<Map<String, Object>> resp;
        if(devDb){
            try{
                resp = devOqgHelper.OqgR2(query, true);
            }catch (Exception e){
                return Map.of("error", e.getMessage());
            }
        }else{
            try{
                resp = oqgHelper.OqgR2(query, true);
            }catch (Exception e){
                return Map.of("error", e.getMessage());
            }
        }


        if(resp.isEmpty()){
            return Map.of("info", "no data found");
        }

        result.put("data", resp);

        return result;
    }
}
