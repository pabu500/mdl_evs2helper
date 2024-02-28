package com.pabu5h.evs2.evs2helper.report;

import com.pabu5h.evs2.evs2helper.locale.LocalHelper;
import com.xt.utils.ExcelUtil;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Component
public class ReportHelper {
    Logger logger = Logger.getLogger(ReportHelper.class.getName());

    @Autowired
    private LocalHelper localHelper;

    public Map<String, Object> genReportInfo(String reportName, String fileExtension) {
        logger.info("genReportName() called");

        File currDir = new File(".");
        String path = currDir.getAbsolutePath();
        String localNowStr = localHelper.getLocalNowStr();
        String timeFix = localNowStr.replace(":", "-").replace(" ", "-");
        String fullReportName = reportName +"-"+timeFix;
        String fileLocation = path.substring(0, path.length() - 1) + fullReportName + "." + fileExtension;

        return Map.of("file_location", fileLocation, "full_report_name", fullReportName);
    }

    public Map<String, Object> genReportExcel(
            String reportName,
            List<LinkedHashMap<String, Object>> report,
            LinkedHashMap<String, Integer> headerMap,
            String sheetName) {
        logger.info("genReportName() called");

        Workbook workbook = ExcelUtil.createWorkbook(
                sheetName,
                headerMap,
                null,
                null);
        Sheet sheet = workbook.getSheet(sheetName);
        sheet.createFreezePane(0, 1);

        ExcelUtil.addRows(workbook, sheetName, report);

        Map<String, Object> reportInfo = genReportInfo(reportName, "xlsx");
        String fileLocation = (String) reportInfo.get("file_location");
        String fullReportName = (String) reportInfo.get("full_report_name");

        File file = new File(fileLocation);
        ExcelUtil.saveWorkbook(workbook, fileLocation);

        Map<String, Object> result = new HashMap<>();
        result.put("file", file);
        result.put("full_report_name", fullReportName);
        return result;
    }

    public Map<String, Object> genReportExcelMultiSheet(String reportName, List<Map<String, Object>> multiSheetReportInfo) {
        logger.info("genReportExcelMultiSheet() called");

        Workbook workbook = ExcelUtil.createWorkbookEmpty();

        for(Map<String, Object> sheetInfo : multiSheetReportInfo) {
            String sheetName = (String) sheetInfo.get("sheet_name");
            List<LinkedHashMap<String, Object>> reportSheet = (List<LinkedHashMap<String, Object>>) sheetInfo.get("report");
            LinkedHashMap<String, Integer> header = (LinkedHashMap<String, Integer>) sheetInfo.get("header");
            ExcelUtil.addSheet(workbook, sheetName, header, reportSheet, null, null);
            LinkedHashMap<String, Object> patch = (LinkedHashMap<String, Object>) sheetInfo.get("patch");
            if(patch != null) {
                ExcelUtil.addPatch(workbook, sheetName, patch);
            }
        }

        Map<String, Object> reportInfo = genReportInfo(reportName, "xlsx");
        String fileLocation = (String) reportInfo.get("file_location");
        String fullReportName = (String) reportInfo.get("full_report_name");

        File file = new File(fileLocation);
        ExcelUtil.saveWorkbook(workbook, fileLocation);

        Map<String, Object> result = new HashMap<>();
        result.put("file", file);
        result.put("full_report_name", fullReportName);
        return result;
    }
}
