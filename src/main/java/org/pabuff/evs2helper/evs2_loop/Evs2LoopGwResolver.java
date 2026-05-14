package org.pabuff.evs2helper.evs2_loop;

import org.pabuff.dto.StdErrorDto;
import org.pabuff.evs2helper.project_ore.ProjectOreHelper;
import org.pabuff.oqghelper2.OqgHelper2;
//import org.pabuff.paghelper.project_ore.ProjectOreHelper;
import org.pabuff.utils.ApiCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class Evs2LoopGwResolver {
    Logger logger = Logger.getLogger(Evs2LoopGwResolver.class.getName());

    @Autowired
    private OqgHelper2 oqgHelper;

    public Map<String, Object> resolveGw(Map<String, Object> request) {
        logger.info("resolveGw()");

        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
        if (scope == null || scope.isEmpty()) {
            return Map.of("error", StdErrorDto.builder()
                    .code(ApiCode.REQUEST_MISSING_PARAMETER)
            .message("scope is required")
            .build());
        }

        String projectName = (String) scope.get("project_name");
        if (projectName == null || projectName.isEmpty()) {
            return Map.of("error", StdErrorDto.builder()
                    .code(ApiCode.REQUEST_MISSING_PARAMETER)
                    .message("project_name is required in scope")
                    .build());
        }
        String projectGwTableName = ProjectOreHelper.getProjectItemTableName(projectName, "gateway");
        String projectMeterTableName = ProjectOreHelper.getProjectItemTableName(projectName, "meter");
        String projectMeterGroupTableName = ProjectOreHelper.getProjectItemTableName(projectName, "meter_group");
        String projectLocationTableName = ProjectOreHelper.getProjectItemTableName(projectName, "location");
        String projectLocationGroupTableName = ProjectOreHelper.getProjectItemTableName(projectName, "location_group");
        String projectBuildingTableName = ProjectOreHelper.getProjectItemTableName(projectName, "building");
        String projectSiteTableName = ProjectOreHelper.getProjectItemTableName(projectName, "site");

        String meterSn = (String) request.get("meter_sn");
        if (meterSn == null || meterSn.isEmpty()) {
            return Map.of("error", StdErrorDto.builder()
                    .code(ApiCode.REQUEST_MISSING_PARAMETER)
                    .message("meter_sn is required in scope")
                    .build());
        }

        String sql = "SELECT gw.id AS gw_id, gw.name AS gw_name, gw.label AS gw_label, gw.tag AS gw_tag, gw.iccid AS gw_iccid, gw.ip AS gw_ip, s.name as gw_site_name " +
                "FROM " + projectGwTableName + " gw " +
                "JOIN " + projectMeterGroupTableName + " mg ON gw.id = mg.gateway_id " +
                "JOIN " + projectMeterTableName + " m ON mg.id = m.meter_group_id " +
                "JOIN " + projectLocationTableName + " l ON gw.location_id = l.id " +
                "JOIN " + projectLocationGroupTableName + " lg ON l.location_group_id = lg.id " +
                "JOIN " + projectBuildingTableName + " b ON lg.building_id = b.id " +
                "JOIN " + projectSiteTableName + " s ON b.site_id = s.id " +
                "WHERE m.meter_sn = '" + meterSn + "' ";
//                "LIMIT 1";
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2x(sql, "OPS", true);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", StdErrorDto.builder()
                    .code(ApiCode.RESULT_DATABASE_ERROR)
                    .message(e.getMessage())
                    .build());
        }
        if (resp.isEmpty()) {
            return Map.of("error", StdErrorDto.builder()
                    .code(ApiCode.RESULT_NOT_FOUND)
                    .message("No gateway found for meter_sn: " + meterSn)
                    .build());
        }
        if (resp.size() > 1) {
            logger.warning("Multiple gateways found for meter_sn: " + meterSn);
            return Map.of("error", StdErrorDto.builder()
                    .code(ApiCode.RESULT_GENERIC_ERROR)
                    .message("Multiple gateways found for meter_sn: " + meterSn)
                    .build());
        }
        return Map.of("gateway_info", resp.getFirst());
    }
}
