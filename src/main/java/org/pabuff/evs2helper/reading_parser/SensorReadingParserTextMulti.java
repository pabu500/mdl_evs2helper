package org.pabuff.evs2helper.reading_parser;

import org.pabuff.dto.SensorReadingMultiDto;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.logging.Logger;

@Component
public class SensorReadingParserTextMulti extends IotReadingParserText<SensorReadingMultiDto> {
    Logger logger = Logger.getLogger(SensorReadingParserTextMulti.class.getName());

    public SensorReadingParserTextMulti() {
        setLookupTable();
    }
    void setLookupTable() {
        lookupTable = new HashMap<>();
        lookupTable.put("3000", "temperature");
        lookupTable.put("3001", "humidity");
    }

    @Override
    public SensorReadingMultiDto parse(String message) {
        SensorReadingMultiDto dto = new SensorReadingMultiDto();

        String cleanedMessage = preClean(message);
        String[] lines = cleanedMessage.split(";");

        //get id from first line id:SMRT_001
        int fieldIndex = 0;
        String type = "";
        if(lines.length > 0){
            String[] parts = lines[fieldIndex++].split(":");
            if(parts.length > 1){
                type = parts[1].trim();
            }
        }
        String itemId = "";
        if(lines.length > 0){
            String[] parts = lines[fieldIndex++].split(":");
            if(parts.length > 1){
                itemId = parts[1].trim();
            }
        }
        dto.setItemName(itemId);
        //get dt from second line dt:2023-10-24T15:37:59
        LocalDateTime dt = null;
        if(lines.length > 1){
            String timestampPart = lines[fieldIndex++].replace("dt:", "").trim();
            try {
                dt = LocalDateTime.parse(timestampPart);
            }catch (Exception e) {
                logger.severe(e.getMessage());
            }
        }
        dto.setDt(dt);

        //get the rest of the lines
        for(int i=fieldIndex; i<lines.length; i++){
            String[] parts = lines[i].split(",");
            if(parts.length < 3){
                continue;
            }
            String keyCode = parts[0].trim();
            String valueType = parts[1].trim();
            String value = parts[2].trim();
            if(lookupTable.containsKey(keyCode)){
                setPropertyValue(dto, keyCode, value, lookupTable, logger);
            }
        }
        return dto;
    }
}
