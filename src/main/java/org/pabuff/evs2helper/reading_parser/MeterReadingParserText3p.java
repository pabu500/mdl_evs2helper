package org.pabuff.evs2helper.reading_parser;

import org.pabuff.dto.MeterReading3pDto;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.logging.Logger;

/* sample message:

id:SMRT_BVI_WP058784
dt:2023-10-24T15:37:59
1107	(V)	9.841999053955078
1109	(V)	48909.0625
1111	(V)	48927.796875
1113	(A)	0.004976562224328518
1115	(A)	0.007014160044491291
1117	(A)	0.016112303361296654
1121	()	1.0
1123	(Hz)	50.05500030517578
1125	(kW)	0.298828125
1127	(kVAr)	-0.7015625238418579
1129	(kVA)	0.7015625238418579
1405	(kWh)	6.56802603213685e-41
1407	(kWh)	0.0
1433	(kVArh)	0.0
1435	(kVArh)	6.56802603213685e-41
2007	(kW)	6.56802603213685e-41
2009	(kW)	0.0

*/

@Service
public class MeterReadingParserText3p extends IotReadingParserText<MeterReading3pDto>{
    Logger logger = Logger.getLogger(MeterReadingParserText3p.class.getName());

    public MeterReadingParserText3p() {
        setLookupTable();
    }

    void setLookupTable() {
        lookupTable = new HashMap<>();
        lookupTable.put("1107", "ptpVoltageL1");
        lookupTable.put("1109", "ptpVoltageL2");
        lookupTable.put("1111", "ptpVoltageL3");
        lookupTable.put("1113", "lineCurrentL1");
        lookupTable.put("1115", "lineCurrentL2");
        lookupTable.put("1117", "lineCurrentL3");
        lookupTable.put("1121", "allPhasePowerFactor");
        lookupTable.put("1123", "frequency");
        lookupTable.put("1125", "allPhaseActivePowerTotal");
        lookupTable.put("1127", "allPhaseReactivePower");
        lookupTable.put("1129", "allPhaseApparentPower");
        lookupTable.put("1405", "activeImport");
        lookupTable.put("1407", "activeExport");
        lookupTable.put("1433", "reactiveLag");
        lookupTable.put("1435", "reactiveLead");
        lookupTable.put("2007", "currentMaxDemandSinceBillingActiveImport");
        lookupTable.put("2009", "currentMaxDemandSinceBillingActiveExport");
    }

    public MeterReading3pDto parse(String message) {
        MeterReading3pDto dto = new MeterReading3pDto();

        String cleanedMessage = preClean(message);
        String[] lines = cleanedMessage.split(";");

        //get id from first line id:SMRT_BVI_WP058784
        String meterId = "";
        if(lines.length > 0){
            String[] parts = lines[0].split(":");
            if(parts.length > 1){
                meterId = parts[1].trim();
            }
        }
        dto.setMeterId(meterId);
        //get dt from second line dt:2023-10-24T15:37:59
        LocalDateTime dt = null;
        if(lines.length > 1){
//            String[] parts = lines[1].split(":");
//            if(parts.length > 1){
//                dt = LocalDateTime.parse(parts[1].trim());
//            }
            String timestampPart = lines[1].replace("dt:", "").trim();
            try {
                dt = LocalDateTime.parse(timestampPart);
            }catch (Exception e) {
                logger.severe(e.getMessage());
            }
        }
        dto.setDt(dt);

        //get the rest of the lines
        for(int i=2; i<lines.length; i++){
            String[] parts = lines[i].split(",");
            if(parts.length < 3){
                continue;
            }
            String keyCode = parts[0].trim();
            String valueType = parts[1].trim();
            String value = parts[2].trim();
            if(lookupTable.containsKey(keyCode)){
//                String propertyName = LOOKUP_TABLE.get(keyCode);
                setPropertyValue(dto, keyCode, value, lookupTable, logger);
            }
        }
        return dto;
    }
}