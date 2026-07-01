package org.pabuff.evs2helper.scope;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.pabuff.dto.PagScopeTypeEnum;

import java.util.Map;

@Builder
@Getter@Setter
public class PagScope {
    String scopeKey;

    String projectName;
    String projectId;

    String siteGroupName;
    String siteGroupId;
    Boolean isAllSiteGroups;

    String siteName;
    String siteId;
    Boolean isAllSites;

    String buildingName;
    String buildingId;
    Boolean isAllBuildings;

    String locationGroupName;
    String locationGroupId;
    Boolean isAllLocationGroups;

    String locationName;
    String locationId;
    Boolean isAllLocations;

    String errorMessage;

    //from Map
    public static PagScope fromMap(Map<String, Object> request) {
        String projectName = (String) request.get("project_name");
        if (projectName == null) {
            return PagScope.builder()
                   .errorMessage("Invalid request. project_name is required.")
                   .build();
        }
        String projectId = (String) request.get("project_id");
//        if (projectId == null) {
//            return PagScope.builder()
//                   .errorMessage("Invalid request. project_id is required.")
//                   .build();
//        }

        String siteGroupName = (String) request.get("site_group_name");
        String siteGroupId = (String) request.get("site_group_id");
        if (siteGroupName != null || siteGroupId != null) {
            if (siteGroupName == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. site_group_name is required.")
                       .build();
            }
            if (siteGroupId == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. site_group_id is required.")
                       .build();
            }
        }

        boolean isAllSiteGroups = false;
        if(siteGroupId == null || siteGroupId.isEmpty()) {
            isAllSiteGroups = true;
        }

        String siteName = (String) request.get("site_name");
        String siteId = (String) request.get("site_id");
        if (siteName != null || siteId != null) {
            if (siteName == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. site_name is required.")
                       .build();
            }
            if (siteId == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. site_id is required.")
                       .build();
            }
        }

        boolean isAllSites = false;
        if(siteId == null || siteId.isEmpty()) {
            isAllSites = true;
        }

        String buildingName = (String) request.get("building_name");
        String buildingId = (String) request.get("building_id");
        if (buildingName != null || buildingId != null) {
            if (buildingName == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. building_name is required.")
                       .build();
            }
            if (buildingId == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. building_id is required.")
                       .build();
            }
        }

        boolean isAllBuildings = false;
        if(buildingId == null || buildingId.isEmpty()) {
            isAllBuildings = true;
        }

        String locationGroupName = (String) request.get("location_group_name");
        String locationGroupId = (String) request.get("location_group_id");
        if (locationGroupName != null || locationGroupId != null) {
            if (locationGroupName == null) {
                return PagScope.builder()
                        .errorMessage("Invalid request. location_group_name is required.")
                        .build();
            }
            if (locationGroupId == null) {
                return PagScope.builder()
                        .errorMessage("Invalid request. location_group_id is required.")
                        .build();
            }
        }

        boolean isAllLocationGroups = false;
        if(locationGroupId == null || locationGroupId.isEmpty()) {
            isAllLocationGroups = true;
        }

        String locationName = (String) request.get("location_name");
        String locationId = (String) request.get("location_id");
        if (locationName != null || locationId != null) {
            if (locationName == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. location_name is required.")
                       .build();
            }
            if (locationId == null) {
                return PagScope.builder()
                       .errorMessage("Invalid request. location_id is required.")
                       .build();
            }
        }

        boolean isAllLocations = false;
        if(locationId == null || locationId.isEmpty()) {
            isAllLocations = true;
        }

        // build cache key
        String scopeKey = projectName;
        if (siteGroupName != null) {
            scopeKey += "_" + siteGroupName;
        }
        if (siteName != null) {
            scopeKey += "_" + siteName;
        }
        if (buildingName != null) {
            scopeKey += "_" + buildingName;
        }
        if (locationGroupName != null) {
            scopeKey += "_" + locationGroupName;
        }
        if (locationName != null) {
            scopeKey += "_" + locationName;
        }

        return PagScope.builder()
                .projectName((String) request.get("project_name"))
                .projectId((String) request.get("project_id"))
                .siteGroupName((String) request.get("site_group_name"))
                .siteGroupId((String) request.get("site_group_id"))
                .isAllSiteGroups(isAllSiteGroups)
                .siteName((String) request.get("site_name"))
                .siteId((String) request.get("site_id"))
                .isAllSites(isAllSites)
                .buildingName((String) request.get("building_name"))
                .buildingId((String) request.get("building_id"))
                .isAllBuildings(isAllBuildings)
                .locationGroupName((String) request.get("location_group_name"))
                .locationGroupId((String) request.get("location_group_id"))
                .isAllLocationGroups(isAllLocationGroups)
                .locationName((String) request.get("location_name"))
                .locationId((String) request.get("location_id"))
                .scopeKey(scopeKey)
                .build();
    }

    public boolean isLocationScope() {
        return locationName != null;
    }
    public boolean isLocationGroupScope() {
        return locationGroupName != null && !isLocationScope();
    }
    public boolean isBuildingScope() {
        return buildingName != null && !isLocationGroupScope();
    }
    public boolean isSiteScope() {
        return siteName != null && !isBuildingScope();
    }
    public boolean isSiteGroupScope() {
        return siteGroupName != null && !isSiteScope();
    }
    public boolean isProjectScope() {
        return projectName != null && !isSiteGroupScope();
    }

    public PagScopeTypeEnum getScopeType() {
        if (isLocationScope()) {
            return PagScopeTypeEnum.location;
        }
        if (isLocationGroupScope()) {
            return PagScopeTypeEnum.locationGroup;
        }
        if (isBuildingScope()) {
            return PagScopeTypeEnum.building;
        }
        if (isSiteScope()) {
            return PagScopeTypeEnum.site;
        }
        if (isSiteGroupScope()) {
            return PagScopeTypeEnum.siteGroup;
        }
        if (isProjectScope()) {
            return PagScopeTypeEnum.project;
        }
        return PagScopeTypeEnum.none;
    }

    public String getScopeLabel() {
        if (isLocationScope()) {
            return locationName;
        }
        if (isLocationGroupScope()) {
            return locationGroupName;
        }
        if (isBuildingScope()) {
            return buildingName;
        }
        if (isSiteScope()) {
            return siteName;
        }
        if (isSiteGroupScope()) {
            return siteGroupName;
        }
        if (isProjectScope()) {
            return projectName;
        }
        return "None";
    }

    public Map<String, Object> getLeafScopeMap() {
        if (isLocationScope()) {
            return Map.of("location_name", locationName, "location_id", locationId);
        }
        if (isLocationGroupScope()) {
            return Map.of("location_group_name", locationGroupName, "location_group_id", locationGroupId);
        }
        if (isBuildingScope()) {
            return Map.of("building_name", buildingName, "building_id", buildingId);
        }
        if (isSiteScope()) {
            return Map.of("site_name", siteName, "site_id", siteId);
        }
        if (isSiteGroupScope()) {
            return Map.of("site_group_name", siteGroupName, "site_group_id", siteGroupId);
        }
        if (isProjectScope()) {
            return Map.of("project_name", projectName, "project_id", projectId);
        }
        return Map.of();
    }
}
