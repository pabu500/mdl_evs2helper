package org.pabuff.evs2helper.device;

import org.pabuff.oqghelper.OqgHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
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
}
