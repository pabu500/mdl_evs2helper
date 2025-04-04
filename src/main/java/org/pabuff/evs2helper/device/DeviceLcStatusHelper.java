package org.pabuff.evs2helper.device;

import org.pabuff.dto.DeviceLcStatusEnum;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.logging.Logger;

@Component
public class DeviceLcStatusHelper {
    Logger logger = Logger.getLogger(DeviceLcStatusHelper.class.getName());

    private final static Set<String> deviceLcStatusDict = Set.of(
            "commission_in_progress", "cip",
            "normal",
            "maintenance", "maint", "maint.",
            "decommissioned", "dc",
            "bypassed", "byp"
    );

    public boolean isValidDeviceLcStatusStr(String status) {
        if (status == null) {
            return false;
        }
        return deviceLcStatusDict.contains(status.toLowerCase());
    }

    public DeviceLcStatusEnum getDeviceLcStatusEnum(String status) {
        if (status == null) {
            return null;
        }
        if (!isValidDeviceLcStatusStr(status)) {
            return null;
        }
        return switch (status.toLowerCase()) {
            case "commission_in_progress", "cip" -> DeviceLcStatusEnum.COMMISSION_IN_PROGRESS;
            case "normal" -> DeviceLcStatusEnum.NORMAL;
            case "maintenance", "maint", "maint."  -> DeviceLcStatusEnum.MAINTENANCE;
            case "decommissioned", "dc" -> DeviceLcStatusEnum.DECOMMISSIONED;
            case "bypassed", "byp" -> DeviceLcStatusEnum.BYPASSED;
            default -> null;
        };
    }

    public String getDeviceLcStatusDbStrFromEnum(DeviceLcStatusEnum status) {
        if (status == null) {
            return null;
        }
        return switch (status) {
            case COMMISSION_IN_PROGRESS -> "cip";
            case NORMAL -> "normal";
            case MAINTENANCE -> "maint";
            case DECOMMISSIONED -> "dc";
            case BYPASSED -> "bypassed";
            case MARKED_FOR_DELETE -> "mfd";
        };
    }

    public String getDeviceLcStatusDbStr(String status) {
        if (status == null) {
            return null;
        }
        if (!isValidDeviceLcStatusStr(status)) {
            return null;
        }
        DeviceLcStatusEnum statusEnum = getDeviceLcStatusEnum(status);
        return getDeviceLcStatusDbStrFromEnum(statusEnum);
    }
}
