package org.pabuff.evs2helper.scope;

import org.pabuff.evs2helper.event.OpResultEvent;
import org.pabuff.evs2helper.event.OpResultEvent2;
import org.pabuff.evs2helper.event.OpResultPublisher;
import org.pabuff.evs2helper.project_ore.ProjectOreHelper;
import org.pabuff.oqghelper2.OqgHelper2;
import org.pabuff.oqghelper2.QueryHelper2;
//import org.pabuff.paghelper.event.OpResultEvent;
//import org.pabuff.paghelper.event.OpResultPublisher;
//import org.pabuff.paghelper.project_ore.ProjectOreHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.logging.Logger;

@Component
public class ScopeProcessor {
    Logger logger = Logger.getLogger(ScopeProcessor.class.getName());

    @Autowired
    private OqgHelper2 oqgHelper;
    @Autowired
    private QueryHelper2 queryHelper;
    @Autowired
    private OpResultPublisher opResultPublisher;

    private final String projectTable = "pag.pag_project";
    private final String projectOreTable = "pag.pag_project_ore";

    public Map<String, Object> getProjectOreRequest(Map<String, Object> request) {
        logger.info("getProjectOreRequest()");

        String projectId = (String) request.get("project_id");
        String projectName = (String) request.get("project_name");

        if (projectId == null || projectId.isEmpty()) {
            logger.info("project_id is null or empty");

            if (projectName == null || projectName.isEmpty()) {
                return Map.of("error", "No project id or name found");
            }

            //query for project id by project name
            String query = "SELECT id FROM " + projectTable + " WHERE name = '" + projectName + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(query, "OPS", true);
                if (resp.isEmpty()) {
                    return Map.of("error", "No project found");
                }
                projectId = (String) resp.getFirst().get("id");
            } catch (Exception e) {
                logger.severe(e.getMessage());
                return Map.of("error", e.getMessage());
            }
        }
        Map<String, Object> mutableRequest = new HashMap<>(request);
        mutableRequest.put("project_id", projectId);

        PagScope pagScope = PagScope.fromMap(mutableRequest);
        if(pagScope.getErrorMessage() != null){
            logger.severe(pagScope.getErrorMessage());
            return Map.of("error", pagScope.getErrorMessage());
        }

        return getProjectOreCore(pagScope);
    }

    public Map<String, Object> getProjectOreRequest2(PagScope pagScope) {
        logger.info("getProjectOreRequest()");

        String projectId = pagScope.getProjectId();
        String projectName = pagScope.getProjectName();

        if (projectId == null || projectId.isEmpty()) {
            logger.info("project_id is null or empty");

            if (projectName == null || projectName.isEmpty()) {
                return Map.of("error", "No project id or name found");
            }

            //query for project id by project name
            String query = "SELECT id FROM " + projectTable + " WHERE name = '" + projectName + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(query, "OPS", true);
                if (resp.isEmpty()) {
                    return Map.of("error", "No project found");
                }
                projectId = (String) resp.getFirst().get("id");
            } catch (Exception e) {
                logger.severe(e.getMessage());
                return Map.of("error", e.getMessage());
            }
        }
//        Map<String, Object> mutableRequest = new HashMap<>(request);
//        mutableRequest.put("project_id", projectId);
//
//        PagScope pagScope = PagScope.fromMap(mutableRequest);
        if(pagScope.getErrorMessage() != null){
            logger.severe(pagScope.getErrorMessage());
            return Map.of("error", pagScope.getErrorMessage());
        }

        return getProjectOreCore(pagScope);
    }

    public Map<String, Object> getProjectOre(PagScope pagScope) {
        logger.info("getProjectOre");
        return getProjectOreCore(pagScope);
    }

    private Map<String, Object> getProjectOreCore(PagScope pagScope) {
        logger.info("getProjectOreCore()");

        String projectId = pagScope.getProjectId();

        String idConstraint = " project_id = " + projectId;
        String query = "SELECT * FROM " + projectOreTable + " WHERE " + idConstraint;
        List<Map<String, Object>> resp;
        try {
            resp = oqgHelper.OqgR2x(query, "OPS", true);
            if (resp.isEmpty()) {
                logger.info("No project found with project_id " + projectId);
                return Map.of("error", "No project found");
            }
            return resp.getFirst();
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    public Map<String, Object> getRoleScope(Map<String, Object> request) {
        logger.info("getRoleScope()");

        String opName = (String) request.get("op_name");
        if (opName == null || opName.isEmpty()) {
            return Map.of("error", "No op_name found");
        }

        float startingProgress = request.get("starting_progress") == null ? 0.0f : (float) request.get("starting_progress");
        float roleScopeProgressLoad = request.get("progress_load") == null ? 100.0f : (float) request.get("progress_load");

        String scopeTableName = (String) request.get("scope_table_name");
        if (scopeTableName == null || scopeTableName.isEmpty()) {
            return Map.of("error", "No scope_table_name found");
        }
        String roleScopeTableName = (String) request.get("role_scope_table_name");
        if (roleScopeTableName == null || roleScopeTableName.isEmpty()) {
            return Map.of("error", "No role_scope_table_name found");
        }
        String roleId = (String) request.get("role_id");
        if (roleId == null || roleId.isEmpty()) {
            return Map.of("error", "No role_id found");
        }

        // load scope till location_group by default
        String lazyLoadScope = (String) request.get("lazy_load_scope");
        if (lazyLoadScope == null || lazyLoadScope.isEmpty()) {
            lazyLoadScope = "location_group";
        }

//        String populateBuildingList = (String) request.get("populate_building_list");
//        if (populateBuildingList == null || populateBuildingList.isEmpty()) {
//            populateBuildingList = "false";
//        }
        Map<String, Object> scopeTableNameInfo = Map.of(
                "site_group_table_name", request.get("site_group_table_name"),
                "site_table_name", request.get("site_table_name"),
                "building_table_name", request.get("building_table_name"),
//                "populate_building_list", populateBuildingList,
                "location_group_table_name", request.get("location_group_table_name"),
                "location_table_name", request.get("location_table_name")
        );

        String sqlScope = "SELECT s.id as scope_id, s.project_id, s.site_group_id, s.site_id, s.building_id, s.location_group_id, s.location_id"
                + " FROM " + roleScopeTableName + " AS rs"
                + " INNER JOIN " + scopeTableName + " AS s ON rs.scope_id = s.id"
                + " WHERE role_id = '" + roleId + "'"
                + " AND (s.is_active IS NULL OR s.is_active = true)";
        List<Map<String, Object>> respScopeList;
        try {
            respScopeList = oqgHelper.OqgR2x(sqlScope, "OPS", true);
        } catch (Exception e) {
            logger.severe(e.getMessage());
            return Map.of("error", e.getMessage());
        }
        if (respScopeList.isEmpty()) {
            return Map.of("error", "No role scope found");
        }

        boolean isAllSites = true;

        for (Map<String, Object> scope : respScopeList) {
            String projectId = (String) scope.get("project_id");
            String siteGroupId = (String) scope.get("site_group_id");
            String siteId = (String) scope.get("site_id");
            String buildingId = (String) scope.get("building_id");
            String locationGroupId = (String) scope.get("location_group_id");
            String locationId = (String) scope.get("location_id");

            boolean emptyProject = projectId == null || projectId.isEmpty();
            boolean emptySiteGroup = siteGroupId == null || siteGroupId.isEmpty();
            boolean emptySite = siteId == null || siteId.isEmpty();
            boolean emptyBuilding = buildingId == null || buildingId.isEmpty();
            boolean emptyLocationGroup = locationGroupId == null || locationGroupId.isEmpty();
            boolean emptyLocation = locationId == null || locationId.isEmpty();

            // if site group is not empty, it means all sites in the site group
            // if site is not empty, it means all buildings in the site
            // if building is not empty, it means all location groups in the building
            // if location group is not empty, it means all locations in the location group

            //cannot be all empty
            if (emptyProject && emptySiteGroup && emptySite && emptyBuilding && emptyLocationGroup && emptyLocation) {
                return Map.of("error", "Invalid role scope");
            }
            //can only be one that is not empty
            if ((emptyProject ? 0 : 1) + (emptySiteGroup ? 0 : 1) + (emptySite ? 0 : 1) + (emptyBuilding ? 0 : 1) + (emptyLocationGroup ? 0 : 1) + (emptyLocation ? 0 : 1) != 1) {
                return Map.of("error", "Invalid role scope");
            }

            if (!emptySite || !emptyBuilding || !emptyLocationGroup || !emptyLocation) {
                isAllSites = false;
            }

            PagScope pagScope = PagScope.builder()
                    .projectId(projectId)
                    .siteGroupId(siteGroupId)
                    .siteId(siteId)
                    .buildingId(buildingId)
                    .locationGroupId(locationGroupId)
                    .locationId(locationId)
                    .build();

            if(pagScope.getErrorMessage() != null){
                logger.severe(pagScope.getErrorMessage());
                return Map.of("error", pagScope.getErrorMessage());
            }

            Map<String, Object> scopeInfo = getScopeInfo(pagScope, scopeTableNameInfo);
            if (scopeInfo.get("error") != null) {
                return scopeInfo;
            }
            scope.put("scope_info", scopeInfo.get("scope_info"));
        }

        // validate and consolidate scope settings
        Map<String, Object> resolveScopeResult = resolveScope(respScopeList, scopeTableNameInfo);
        if (resolveScopeResult.get("error") != null) {
            return resolveScopeResult;
        }

        Map<String, Object> resolvedScope = (Map<String, Object>) resolveScopeResult.get("resolved_scope");

        // populate location, location group, building, site, site group info into the scope
        Map<String, Object> populateScopeResult = genScopeTree(
                resolvedScope, scopeTableNameInfo,
                lazyLoadScope,
                opName, startingProgress, roleScopeProgressLoad);

        if (populateScopeResult.get("error") != null) {
            return populateScopeResult;
        }

        Map<String, Object> scopeResult = new LinkedHashMap<>();
        List<Map<String, Object>> populatedScopeList = (List<Map<String, Object>>) populateScopeResult.get("populated_scope");
        scopeResult.put("is_all_sites", Boolean.toString(isAllSites));
        scopeResult.put("scope_list", populatedScopeList);

        return Map.of("role_scope", scopeResult);
    }

    public Map<String, Object> getScopeInfo(PagScope scope, Map<String, Object> scopeTableNameInfo){
        logger.info("getScopeInfo()");

        String projectId = scope.getProjectId();
        String siteGroupId = scope.getSiteGroupId();
        String siteId = scope.getSiteId();
        String buildingId = scope.getBuildingId();
        String locationGroupId = scope.getLocationGroupId();
        String locationId = scope.getLocationId();

        String siteGroupTableName = (String) scopeTableNameInfo.get("site_group_table_name");
        String siteTableName = (String) scopeTableNameInfo.get("site_table_name");
        String buildingTableName = (String) scopeTableNameInfo.get("building_table_name");
        String locationTableName = (String) scopeTableNameInfo.get("location_table_name");
        String locationGroupTableName = (String) scopeTableNameInfo.get("location_group_table_name");
//        String siteGroupSiteTableName = (String) scopeTableNameInfo.get("site_group_site_table_name");

        if (siteGroupId != null && !siteGroupId.isEmpty()) {
            if (siteGroupTableName == null || siteGroupTableName.isEmpty()) {
                return Map.of("error", "No site_group_table_name found");
            }
        }
        if (siteId != null && !siteId.isEmpty()) {
            if (siteTableName == null || siteTableName.isEmpty()) {
                return Map.of("error", "No site_table_name found");
            }
        }
        if (buildingId != null && !buildingId.isEmpty()) {
            if (buildingTableName == null || buildingTableName.isEmpty()) {
                return Map.of("error", "No building_table_name found");
            }
        }
        if (locationGroupId != null && !locationGroupId.isEmpty()) {
            if (locationGroupTableName == null || locationGroupTableName.isEmpty()) {
                return Map.of("error", "No location_group_table_name found");
            }
        }
        if (locationId != null && !locationId.isEmpty()) {
            if (locationTableName == null || locationTableName.isEmpty()) {
                return Map.of("error", "No location_table_name found");
            }
        }

        Map<String, Object> scopeInfo = new LinkedHashMap<>();
        if(locationId != null && !locationId.isEmpty()){
            String sql = "SELECT * FROM " + locationTableName + " WHERE id = '" + locationId + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(sql, "OPS", true);
            } catch (Exception e) {
                return Collections.singletonMap("error", e.getMessage());
            }
            if (resp.isEmpty()) {
                return Collections.singletonMap("error", "location_id not found");
            }
            scopeInfo.put("type", "location");
            scopeInfo.put("item_info", resp.getFirst());
            return Map.of("scope_info", scopeInfo);
        }else{
            if(locationGroupId != null && !locationGroupId.isEmpty()){
                String sql = "SELECT * FROM " + locationGroupTableName + " WHERE id = '" + locationGroupId + "'";
                List<Map<String, Object>> resp;
                try {
                    resp = oqgHelper.OqgR2x(sql, "OPS", true);
                } catch (Exception e) {
                    return Collections.singletonMap("error", e.getMessage());
                }
                if (resp.isEmpty()) {
                    return Collections.singletonMap("error", "location_group_id not found");
                }
                scopeInfo.put("type", "location_group");
                // default to wildcard location group,
                // unless location id of this location group is present in the scope setting
                // which will cause conflict and will not pass validation in ResolveScope
                scopeInfo.put("is_wildcard", "true");
                scopeInfo.put("item_info", resp.getFirst());
                return Map.of("scope_info", scopeInfo);
            }else{
                if(buildingId != null && !buildingId.isEmpty()){
                    String sql = "SELECT * FROM " + buildingTableName + " WHERE id = '" + buildingId + "'";
                    List<Map<String, Object>> resp;
                    try {
                        resp = oqgHelper.OqgR2x(sql, "OPS", true);
                    } catch (Exception e) {
                        return Collections.singletonMap("error", e.getMessage());
                    }
                    if (resp.isEmpty()) {
                        return Collections.singletonMap("error", "building_id not found");
                    }
                    scopeInfo.put("type", "building");
                    scopeInfo.put("is_wildcard", "true");
                    scopeInfo.put("item_info", resp.getFirst());
                    return Map.of("scope_info", scopeInfo);
                }else {
                    if (siteId != null && !siteId.isEmpty()) {
                        String sql = "SELECT * FROM " + siteTableName + " WHERE id = '" + siteId + "'";
                        List<Map<String, Object>> resp;
                        try {
                            resp = oqgHelper.OqgR2x(sql, "OPS", true);
                        } catch (Exception e) {
                            return Collections.singletonMap("error", e.getMessage());
                        }
                        if (resp.isEmpty()) {
                            return Collections.singletonMap("error", "site_id not found");
                        }
                        scopeInfo.put("type", "site");
                        scopeInfo.put("is_wildcard", "true");
                        scopeInfo.put("item_info", resp.getFirst());
                        return Map.of("scope_info", scopeInfo);
                    } else {
                        if (siteGroupId != null && !siteGroupId.isEmpty()) {
                            Map<String, Object> siteGroupInfoResult = queryHelper.getItemInfo(
                                    "id",
                                    siteGroupId,
                                    siteGroupTableName,
                                    "site_group_info");
                            if (siteGroupInfoResult.get("error") != null) {
                                return siteGroupInfoResult;
                            }
                            scopeInfo.put("type", "site_group");
                            scopeInfo.put("is_wildcard", "true");
                            scopeInfo.put("item_info", siteGroupInfoResult.get("site_group_info"));

                            Map<String, Object> result = new LinkedHashMap<>();
                            result.put("scope_info", scopeInfo);
                            return result;
                        } else {
                            return Collections.singletonMap("error", "No scope found");
                        }
                    }
                }
            }
        }
    }

    public Map<String, Object> resolveScope(List<Map<String, Object>> roleScope, Map<String, Object> scopeTableNameInfo) {
        logger.info("resolveScope()");

        List<Map<String, Object>> projectScopeList = new ArrayList<>();
        List<Map<String, Object>> siteGroupScopeList = new ArrayList<>();
        List<Map<String, Object>> siteScopeList = new ArrayList<>();
        List<Map<String, Object>> buildingScopeList = new ArrayList<>();
        List<Map<String, Object>> locationGroupScopeList = new ArrayList<>();
        List<Map<String, Object>> locationScopeList = new ArrayList<>();

        // sort role scope into different scope list
        for (Map<String, Object> scope : roleScope) {
            String projectId = (String) scope.get("project_id");
            String siteGroupId = (String) scope.get("site_group_id");
            String siteId = (String) scope.get("site_id");
            String buildingId = (String) scope.get("building_id");
            String locationGroupId = (String) scope.get("location_group_id");
            String locationId = (String) scope.get("location_id");

            if (projectId != null && !projectId.isEmpty()) {
                projectScopeList.add(scope);
            } else if (siteGroupId != null && !siteGroupId.isEmpty()) {
                siteGroupScopeList.add(scope);
            } else if (siteId != null && !siteId.isEmpty()) {
                siteScopeList.add(scope);
            } else if (buildingId != null && !buildingId.isEmpty()) {
                buildingScopeList.add(scope);
            } else if (locationGroupId != null && !locationGroupId.isEmpty()) {
                locationGroupScopeList.add(scope);
            } else if (locationId != null && !locationId.isEmpty()) {
                locationScopeList.add(scope);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();

        result.put("project_scope_list", projectScopeList);
        result.put("site_group_scope_list", siteGroupScopeList);
        result.put("site_scope_list", siteScopeList);
        result.put("building_scope_list", buildingScopeList);
        result.put("location_group_scope_list", locationGroupScopeList);
        result.put("location_scope_list", locationScopeList);

        // the presents of site_group_id indicates all sites in the site group
        // the presents of site_id indicates all buildings in the site
        // the presents of building_id indicates all location groups in the building
        // the presents of location_group_id indicates all locations in the location group
        // therefore when the parent scope is wildcard, the child scope id should not be present
        // e.g. when site_group_id is present in the list, site_id should not be present in the list

        // check location list and
        // 1. if a location's location group id is in the location group list, return error
        //    as location group scope conflicts with location scope provided by the location list
        // 2. add location_group_id to location
        // wildcard location group id and location id cannot present at the same time

        /* start validating scope setting */

        String locationTableName = (String) scopeTableNameInfo.get("location_table_name");
        List<Map<String, Object>> implicitLocationGroupScopeList = new ArrayList<>();
        for (Map<String, Object> locationScope : locationScopeList) {
            String sql = "SELECT location_group_id FROM " + locationTableName + " WHERE id = '" + locationScope.get("location_id") + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(sql, "OPS", true);
            } catch (Exception e) {
                return Collections.singletonMap("error", e.getMessage());
            }
            if (resp.isEmpty()) {
                return Collections.singletonMap("error", "location id not found");
            }
            String locationGroupId = (String) resp.getFirst().get("location_group_id");
//            locationScope.put("location_group_id", locationGroupId);

            for (Map<String, Object> locationGroup : locationGroupScopeList) {
                if (locationGroup.get("location_group_id").equals(locationGroupId)) {
                    // wildcard location group id and location id cannot present at the same time
                    String msg = "scope " + locationScope.get("scope_id") +
                            " of location " + locationScope.get("location_id") +
                            " conflicts with location group scope " + locationGroupId;
                    logger.severe(msg);
                    return Collections.singletonMap("error", msg);
                }
            }

            // check implicit location group id associated with the location
            // if implicit location group id is not present in the location group list, add it
            boolean locationGroupFound = false;
            for (Map<String, Object> locationGroup : locationGroupScopeList) {
                if (locationGroup.get("location_group_id").equals(locationGroupId)) {
                    locationGroupFound = true;
                    break;
                }
            }
            // if not found, search in the implicit location group scope list
            if (!locationGroupFound) {
                for (Map<String, Object> locationGroup : implicitLocationGroupScopeList) {
                    if (locationGroup.get("location_group_id").equals(locationGroupId)) {
                        locationGroupFound = true;
                        break;
                    }
                }
            }
            // if not found, add implicit location group scope
            if (!locationGroupFound) {
                for (Map<String, Object> locationGroup : implicitLocationGroupScopeList) {
                    if (locationGroup.get("location_group_id").equals(locationGroupId)) {
                        locationGroupFound = true;
                        break;
                    }
                }
            }
            if (!locationGroupFound) {
                Map<String, Object> locationGroupInfoResult = queryHelper.getItemInfo(
                        "id",
                        locationGroupId,
                        (String) scopeTableNameInfo.get("location_group_table_name"),
                        "location_group_info");
                if (locationGroupInfoResult.get("error") != null) {
                    return locationGroupInfoResult;
                }
                Map<String, Object> locationGroupInfo = new LinkedHashMap<>();
                locationGroupInfo.put("location_group_id", locationGroupId);

                Map<String, Object> scopeInfo = new LinkedHashMap<>();
                scopeInfo.put("type", "location_group");
                //location group is not wildcard because location id is present in the scope setting
                scopeInfo.put("is_wildcard", "false");
                scopeInfo.put("is_implicit", "true");
                scopeInfo.put("item_info", locationGroupInfoResult.get("location_group_info"));
                locationGroupInfo.put("scope_info", scopeInfo);

                implicitLocationGroupScopeList.add(locationGroupInfo);
            }
        }
        locationGroupScopeList.addAll(implicitLocationGroupScopeList);

        // check location group list and
        // 1. if a location group's building id is in the building list, return error
        //    as building scope conflicts with location group scope provided by the location group list
        // 2. add building_id to location group
        // wildcard building id and location group id cannot present at the same time
        String locationGroupTableName = (String) scopeTableNameInfo.get("location_group_table_name");
        List<Map<String, Object>> implicitBuildingScopeList = new ArrayList<>();
        for (Map<String, Object> locationGroup : locationGroupScopeList) {
            //find building id from location group id
            String sql = "SELECT building_id FROM " + locationGroupTableName + " WHERE id = '" + locationGroup.get("location_group_id") + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(sql, "OPS", true);
            } catch (Exception e) {
                return Collections.singletonMap("error", e.getMessage());
            }
            if (resp.isEmpty()) {
                return Collections.singletonMap("error", "location group id not found");
            }
            String buildingId = (String) resp.getFirst().get("building_id");
//            locationGroup.put("building_id", buildingId);

            Map<String, Object> locationGroupScopeInfo = (Map<String, Object>) locationGroup.get("scope_info");
            boolean isImplicit = "true".equals(locationGroupScopeInfo.get("is_implicit"));
            String implicitMsg = isImplicit ? " (implicit)" : "";

            // check if building id is in the building list (as wildcard building id) from the scope setting
            for (Map<String, Object> building : buildingScopeList) {
                // wildcard building id and location group id cannot present at the same time
                if (building.get("building_id").equals(buildingId)) {
                    String msg = "scope " + locationGroup.get("scope_id") +
                            " of location group scope " + locationGroup.get("location_group_id") + implicitMsg +
                            " conflicts with building scope " + buildingId;
                    logger.severe(msg);
                    return Collections.singletonMap("error", msg);
                }
            }

            // check implicit building id associated with the location group
            // if implicit building id is not present in the building list, add it to the building list
            boolean buildingFound = false;
            for (Map<String, Object> building : buildingScopeList) {
                if (building.get("building_id").equals(buildingId)) {
                    buildingFound = true;
                    break;
                }
            }
            // if not found, search in the implicit building scope list
            if (!buildingFound) {
                for (Map<String, Object> building : implicitBuildingScopeList) {
                    if (building.get("building_id").equals(buildingId)) {
                        buildingFound = true;
                        break;
                    }
                }
            }
            if (!buildingFound) {
                Map<String, Object> buildingInfoResult = queryHelper.getItemInfo(
                        "id",
                        buildingId,
                        (String) scopeTableNameInfo.get("building_table_name"),
                        "building_info");
                if (buildingInfoResult.get("error") != null) {
                    return buildingInfoResult;
                }
                Map<String, Object> buildingInfo = new LinkedHashMap<>();
                buildingInfo.put("building_id", buildingId);

                Map<String, Object> scopeInfo = new LinkedHashMap<>();
                scopeInfo.put("type", "building");
                scopeInfo.put("is_wildcard", "false");
                scopeInfo.put("is_implicit", "true");
                scopeInfo.put("item_info", buildingInfoResult.get("building_info"));
                buildingInfo.put("scope_info", scopeInfo);

                // use another list to store implicit building scope
                // to avoid
                // 1. concurrent modification
                // 2. the newly added implicit building scope will always conflict with the according location group scope
                implicitBuildingScopeList.add(buildingInfo);
            }
        }
        // add implicit building scope list to building scope list
        buildingScopeList.addAll(implicitBuildingScopeList);

        // check building list and
        // 1. if a building's site id is in the site list, return error
        //    as site scope conflicts with building scope provided by the building list
        // 2. add site_id to building
        // wildcard site id and building id cannot present at the same time
        String buildingTableName = (String) scopeTableNameInfo.get("building_table_name");
        List<Map<String, Object>> implicitSiteScopeList = new ArrayList<>();
        for (Map<String, Object> building : buildingScopeList) {
            String sql = "SELECT site_id FROM " + buildingTableName + " WHERE id = '" + building.get("building_id") + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(sql, "OPS", true);
            } catch (Exception e) {
                return Collections.singletonMap("error", e.getMessage());
            }
            if (resp.isEmpty()) {
                return Collections.singletonMap("error", "building id not found");
            }
            String siteId = (String) resp.getFirst().get("site_id");
//            building.put("site_id", siteId);

            Map<String, Object> buildingScopeInfo = (Map<String, Object>) building.get("scope_info");
            boolean isImplicit = "true".equals(buildingScopeInfo.get("is_implicit"));
            String implicitMsg = isImplicit ? " (implicit)" : "";

            for (Map<String, Object> site : siteScopeList) {
                if (site.get("site_id").equals(siteId)) {
                    // wildcard site id and building id cannot present at the same time
                    String msg = "scope " + building.get("scope_id") + implicitMsg +
                            " of building scope " + building.get("building_id") +
                            " conflicts with site scope " + siteId;
                    logger.severe(msg);
                    return Collections.singletonMap("error", msg);
                }
            }

            // check implicit site id associated with the building
            // if implicit site id is not present in the site list, add it
            boolean siteFound = false;
            for (Map<String, Object> site : siteScopeList) {
                if (site.get("site_id").equals(siteId)) {
                    siteFound = true;
                    break;
                }
            }
            // if not found, search in the implicit site scope list
            if (!siteFound) {
                for (Map<String, Object> site : implicitSiteScopeList) {
                    if (site.get("site_id").equals(siteId)) {
                        siteFound = true;
                        break;
                    }
                }
            }
            if (!siteFound) {
                Map<String, Object> siteInfoResult = queryHelper.getItemInfo(
                        "id",
                        siteId,
                        (String) scopeTableNameInfo.get("site_table_name"),
                        "site_info");
                if (siteInfoResult.get("error") != null) {
                    return siteInfoResult;
                }
                Map<String, Object> siteInfo = new LinkedHashMap<>();
                siteInfo.put("site_id", siteId);

                Map<String, Object> scopeInfo = new LinkedHashMap<>();
                scopeInfo.put("type", "site");
                scopeInfo.put("is_wildcard", "false");
                scopeInfo.put("is_implicit", "true");
                scopeInfo.put("item_info", siteInfoResult.get("site_info"));
                siteInfo.put("scope_info", scopeInfo);

                implicitSiteScopeList.add(siteInfo);
            }
        }
        // add implicit site scope to site scope list
        siteScopeList.addAll(implicitSiteScopeList);

        // check site list and
        // 1. if a site's site group id is in the site group list, return error
        //    as site group scope conflicts with site scope provided by the site list
        // 2. add site_group_id to site
        // wildcard site group id and site id cannot present at the same time
        String siteTableName = (String) scopeTableNameInfo.get("site_table_name");
        List<Map<String, Object>> implicitSiteGroupScopeList = new ArrayList<>();
        for (Map<String, Object> site : siteScopeList) {
            String sql = "SELECT site_group_id FROM " + siteTableName + " WHERE id = '" + site.get("site_id") + "'";
            List<Map<String, Object>> resp;
            try {
                resp = oqgHelper.OqgR2x(sql, "OPS", true);
            } catch (Exception e) {
                return Collections.singletonMap("error", e.getMessage());
            }
            if (resp.isEmpty()) {
                return Collections.singletonMap("error", "site id not found");
            }
            String siteGroupId = (String) resp.getFirst().get("site_group_id");
//            site.put("site_group_id", siteGroupId);

            Map<String, Object> siteScopeInfo = (Map<String, Object>) site.get("scope_info");
            boolean isImplicit = "true".equals(siteScopeInfo.get("is_implicit"));
            String implicitMsg = isImplicit ? " (implicit)" : "";

            for (Map<String, Object> siteGroup : siteGroupScopeList) {
                if (siteGroup.get("site_group_id").equals(siteGroupId)) {
                    // wildcard site group id and site id cannot present at the same time
                    String msg = "scope " + site.get("scope_id") + implicitMsg +
                            " of site scope " + site.get("site_id") +
                            " conflicts with site group scope " + siteGroupId;
                    logger.severe(msg);
                    return Collections.singletonMap("error", msg);
                }
            }
            // check implicit site group id associated with the site
            // if implicit site group id is not present in the site group list, add it
            boolean siteGroupFound = false;
            for (Map<String, Object> siteGroup : siteGroupScopeList) {
                if (siteGroup.get("site_group_id").equals(siteGroupId)) {
                    siteGroupFound = true;
                    break;
                }
            }
            // if not found, search in the implicit site group scope list
            if (!siteGroupFound) {
                for (Map<String, Object> siteGroup : implicitSiteGroupScopeList) {
                    if (siteGroup.get("site_group_id").equals(siteGroupId)) {
                        siteGroupFound = true;
                        break;
                    }
                }
            }
            if (!siteGroupFound) {
                Map<String, Object> siteGroupInfoResult = queryHelper.getItemInfo(
                        "id",
                        siteGroupId,
                        (String) scopeTableNameInfo.get("site_group_table_name"),
                        "site_group_info");
                if (siteGroupInfoResult.get("error") != null) {
                    return siteGroupInfoResult;
                }

                Map<String, Object> siteGroupInfo = new LinkedHashMap<>();
                siteGroupInfo.put("site_group_id", siteGroupId);

                Map<String, Object> scopeInfo = new LinkedHashMap<>();
                scopeInfo.put("type", "site_group");
                scopeInfo.put("is_wildcard", "false");
                scopeInfo.put("is_implicit", "true");
                scopeInfo.put("item_info", siteGroupInfoResult.get("site_group_info"));
                siteGroupInfo.put("scope_info", scopeInfo);

                implicitSiteGroupScopeList.add(siteGroupInfo);
            }
        }
        // add implicit site group scope to site group scope list
        siteGroupScopeList.addAll(implicitSiteGroupScopeList);
        /* end validating scope setting */

        return Map.of("resolved_scope", result);
    }

    // rearrange the scope list from sorted scope lists to a tree structure
    // the tree contains project, site group, site, building, location group
    // no location will be added
    // this is to prevent overloading the tree with too many nodes
    public Map<String, Object> genScopeTree(Map<String, Object>resolvedScope,
                                            Map<String, Object> scopeTableNameInfo,
                                            String lazyLoadScope,
                                            String opName,
                                            Float treeStartingProgress,
                                            Float treeProgressLoad) {
        logger.info("genScopeTree()");

        float progress = treeStartingProgress == null ? 0.0f : treeStartingProgress;
        float progressLoad = treeProgressLoad == null ? 100.0f : treeProgressLoad;
        logger.info("progress: " + progress + ", progressLoad: " + progressLoad);

        List<Map<String, Object>> siteGroupList = (List<Map<String, Object>>) resolvedScope.get("site_group_scope_list");
        try {
            // assemble the tree structure from top down
            float loadStepSiteGroup = progressLoad / siteGroupList.size();
            logger.info("loadStepSiteGroup: " + loadStepSiteGroup);

            for (Map<String, Object> siteGroup : siteGroupList) {
                if(lazyLoadScope.equals("site_group")){
                    progress += loadStepSiteGroup;
                    logger.info("progress: " + progress);
                    continue;
                }

                Map<String, Object> siteGroupScopeInfo = (Map<String, Object>) siteGroup.get("scope_info");
                Map<String, Object> siteGroupInfo = (Map<String, Object>) siteGroupScopeInfo.get("item_info");
                String isWildcardStr = (String) siteGroupScopeInfo.get("is_wildcard");
                if (isWildcardStr == null || isWildcardStr.isEmpty()) {
                    return Collections.singletonMap("error", "is_wildcard not found");
                }
                boolean isWildcard = "true".equals(isWildcardStr);
                List<Map<String, Object>> siteList = new ArrayList<>();
                if (isWildcard) {
                    // get all sites in the site group
                    String siteGroupId = (String) siteGroupInfo.get("id");
                    Map<String, Object> siteGroupSiteListResult = queryHelper.getScopeChildrenList(
                            "site_group_id",
                            siteGroupId,
                            (String) scopeTableNameInfo.get("site_table_name"),
                            "site_list",
                            "site_info");
                    if (siteGroupSiteListResult.get("error") != null) {
                        logger.severe((String) siteGroupSiteListResult.get("error"));
                        return siteGroupSiteListResult;
                    }
                    List<Map<String, Object>> siteInfoList = (List<Map<String, Object>>) siteGroupSiteListResult.get("site_list");
                    //populate scope info with site list
                    for (Map<String, Object> siteInfo : siteInfoList) {
                        Map<String, Object> scopeInfo = new LinkedHashMap<>();
                        scopeInfo.put("type", "site");
                        scopeInfo.put("is_wildcard", "true");
                        scopeInfo.put("item_info", siteInfo);

                        Map<String, Object> siteScopeInfo = new LinkedHashMap<>();
                        siteScopeInfo.put("site_id", siteInfo.get("id"));
                        siteScopeInfo.put("scope_info", scopeInfo);
                        siteList.add(siteScopeInfo);
                    }
                }else{
                    // get from site list in the resolved scope
                    List<Map<String, Object>> siteScopeList = (List<Map<String, Object>>) resolvedScope.get("site_scope_list");
                    for (Map<String, Object> site : siteScopeList) {
                        Map<String, Object> siteScopeInfo = (Map<String, Object>) site.get("scope_info");
                        Map<String, Object> siteInfo = (Map<String, Object>) siteScopeInfo.get("item_info");
                        if (!siteGroupInfo.get("id").equals(siteInfo.get("site_group_id"))) {
                            continue;
                        }
                        siteList.add(site);
                    }
                }
                siteGroupScopeInfo.put("site_list", siteList);

                // populate site info with building info list
                float loadStepSite = loadStepSiteGroup / siteList.size();
                logger.info("loadStepSite: " + loadStepSite);

                for (Map<String, Object> siteScopeInfo : siteList) {
                    Map<String, Object> scopeInfo = (Map<String, Object>) siteScopeInfo.get("scope_info");
                    String type = (String) scopeInfo.get("type");
                    if (!"site".equals(type)) {
                        continue;
                    }
                    List<Map<String, Object>> buildingList = new ArrayList<>();
                    String isBuildingWildcardStr = (String) scopeInfo.get("is_wildcard");
                    if (isBuildingWildcardStr == null || isBuildingWildcardStr.isEmpty()) {
                        logger.severe("is_wildcard not found");
                        return Collections.singletonMap("error", "is_wildcard not found");
                    }
                    boolean isBuildingWildcard = "true".equals(isBuildingWildcardStr);
                    if (isBuildingWildcard) {
                        // if is wildcard, pull all building info with the same site id
                        String siteId = (String) siteScopeInfo.get("site_id");

                        Map<String, Object> siteBuildingListResult = queryHelper.getScopeChildrenList(
                                "site_id",
                                siteId,
                                (String) scopeTableNameInfo.get("building_table_name"),
                                "building_list",
                                "building_info");
                        if (siteBuildingListResult.get("error") != null) {
                            logger.severe((String) siteBuildingListResult.get("error"));
                            return siteBuildingListResult;
                        }
                        List<Map<String, Object>> buildingInfoList = (List<Map<String, Object>>) siteBuildingListResult.get("building_list");
                        //populate scope info with building list
                        for (Map<String, Object> buildingInfo : buildingInfoList) {
                            Map<String, Object> scopeInfo2 = new LinkedHashMap<>();
                            scopeInfo2.put("type", "building");
                            scopeInfo2.put("is_wildcard", "true");
                            scopeInfo2.put("item_info", buildingInfo);

                            Map<String, Object> buildingScopeInfo = new LinkedHashMap<>();
                            buildingScopeInfo.put("building_id", buildingInfo.get("id"));
                            buildingScopeInfo.put("scope_info", scopeInfo2);
                            buildingList.add(buildingScopeInfo);
                        }
                    }else {
                        // get from building list in the resolved scope
                        List<Map<String, Object>> buildingScopeList = (List<Map<String, Object>>) resolvedScope.get("building_scope_list");
                        for (Map<String, Object> building : buildingScopeList) {
                            Map<String, Object> buildingScopeInfo = (Map<String, Object>) building.get("scope_info");
                            Map<String, Object> buildingInfo = (Map<String, Object>) buildingScopeInfo.get("item_info");
                            if (!siteScopeInfo.get("site_id").equals(buildingInfo.get("site_id"))) {
                                continue;
                            }
                            buildingList.add(building);
                        }
                    }
                    scopeInfo.put("building_list", buildingList);

                    // populate building info with location group info list
                    for (Map<String, Object> buildingScopeInfo : buildingList) {
                        Map<String, Object> scopeInfo2 = (Map<String, Object>) buildingScopeInfo.get("scope_info");
                        String type2 = (String) scopeInfo2.get("type");
                        if (!"building".equals(type2)) {
                            continue;
                        }
                        List<Map<String, Object>> locationGroupList = new ArrayList<>();
                        String isLocationGroupWildcardStr = (String) scopeInfo2.get("is_wildcard");
                        if (isLocationGroupWildcardStr == null || isLocationGroupWildcardStr.isEmpty()) {
                            logger.severe("is_wildcard not found");
                            return Collections.singletonMap("error", "is_wildcard not found");
                        }
                        boolean isLocationGroupWildcard = "true".equals(isLocationGroupWildcardStr);
                        if (isLocationGroupWildcard) {
                            // if is wildcard, pull all location group info with the same building id
                            String buildingId = (String) buildingScopeInfo.get("building_id");

                            Map<String, Object> buildingLocationGroupListResult = queryHelper.getScopeChildrenList(
                                    "building_id",
                                    buildingId,
                                    (String) scopeTableNameInfo.get("location_group_table_name"),
                                    "location_group_list",
                                    "location_group_info");
                            if (buildingLocationGroupListResult.get("error") != null) {
                                logger.severe((String) buildingLocationGroupListResult.get("error"));
                                return buildingLocationGroupListResult;
                            }
                            List<Map<String, Object>> locationGroupInfoList = (List<Map<String, Object>>) buildingLocationGroupListResult.get("location_group_list");
                            //populate scope info with location group list
                            for (Map<String, Object> locationGroupInfo : locationGroupInfoList) {
                                Map<String, Object> scopeInfo3 = new LinkedHashMap<>();
                                scopeInfo3.put("type", "location_group");
                                scopeInfo3.put("is_wildcard", "true");
                                scopeInfo3.put("item_info", locationGroupInfo);

                                Map<String, Object> locationGroupScopeInfo = new LinkedHashMap<>();
                                locationGroupScopeInfo.put("location_group_id", locationGroupInfo.get("id"));
                                locationGroupScopeInfo.put("scope_info", scopeInfo3);
                                locationGroupList.add(locationGroupScopeInfo);
                            }
                        } else {
                            // get from location group list in the resolved scope
                            List<Map<String, Object>> locationGroupScopeList = (List<Map<String, Object>>) resolvedScope.get("location_group_scope_list");
                            for (Map<String, Object> locationGroup : locationGroupScopeList) {
                                Map<String, Object> locationGroupScopeInfo = (Map<String, Object>) locationGroup.get("scope_info");
                                Map<String, Object> locationGroupInfo = (Map<String, Object>) locationGroupScopeInfo.get("item_info");
                                if (!buildingScopeInfo.get("building_id").equals(locationGroupInfo.get("building_id"))) {
                                    continue;
                                }
                                locationGroupList.add(locationGroup);
                            }
                        }
                        scopeInfo2.put("location_group_list", locationGroupList);
                    }

                    progress += loadStepSite;
                    logger.info("progress: " + progress);
                    opResultPublisher.publishEvent2(OpResultEvent2.builder()
                            .opName(opName)
                            .message("progress")
                            .progress(progress)
                            .build());
                }
            }
        }catch (Exception e){
            logger.severe(e.getMessage());
            return Collections.singletonMap("error", e.getMessage());
        }
        return Map.of("populated_scope", siteGroupList);
    }

    // add flex scope to pag or project item
    public Map<String, Object> getFlexiItemScopeJoinOnList(Map<String, Object> request) {
        logger.info("getFlexiItemScopeJoinOnList()");

        Map<String, Object> scope = (Map<String, Object>) request.get("scope");
        if(scope == null){
            return Map.of("error", "scope is required");
        }
        PagScope pagScope = PagScope.fromMap(scope);
        if(pagScope.getErrorMessage() != null){
            return Map.of("error", pagScope.getErrorMessage());
        }

        String projectName = pagScope.getProjectName();
        String projectIdStr = pagScope.getProjectId();

        String projectItemName = (String) request.get("project_item_name");
        String pagItemName = (String) request.get("pag_item_name");
        String projectItemTableName = (String) request.get("project_item_table_name");

        if((pagItemName == null || pagItemName.isEmpty()) && (projectItemName == null || projectItemName.isEmpty()) && (projectItemTableName == null || projectItemTableName.isEmpty())) {
            return Map.of("error", "pag_item_name or project_item_name or project_item_table_name is required");
        }

//        String projectItemTableName = "";
        if(projectItemTableName== null || projectItemTableName.isEmpty()) {
            if (projectItemName != null && !projectItemName.isEmpty()) {
                projectItemTableName = ProjectOreHelper.getProjectItemTableName(projectName, projectItemName);
            }
        }

        String pagItemTableName = "";
        if(pagItemName != null && !pagItemName.isEmpty()) {
            pagItemTableName = "pag.pag_" + pagItemName;
        }
        String pagItemScopeTableName = "";
        if(pagItemName != null && !pagItemName.isEmpty()) {
            pagItemScopeTableName = ProjectOreHelper.getProjectItemTableName(projectName, pagItemName + "_scope");
        }

        String projectScopeTableName = ProjectOreHelper.getProjectItemTableName(projectName, "scope");
        String projectLocationTableName = ProjectOreHelper.getProjectItemTableName(projectName, "location");
        String projectLocationGroupTableName = ProjectOreHelper.getProjectItemTableName(projectName, "location_group");
        String projectBuildingTableName = ProjectOreHelper.getProjectItemTableName(projectName, "building");
        String projectSiteTableName = ProjectOreHelper.getProjectItemTableName(projectName, "site");
        String projectSiteGroupTableName = ProjectOreHelper.getProjectItemTableName(projectName, "site_group");

        // is project item
//        String scopeIdScopeSql = "SELECT scope_id FROM " + projectItemTableName;

        Map<String, Object> joinPagItemScope = new HashMap<>();
        Map<String, Object> joinProjectItemProjectScope = new HashMap<>();

        if(projectItemTableName.isEmpty()) {
            // is pag item
//            scopeIdScopeSql = "SELECT scope_id FROM " + pagItemScopeTableName;

            joinPagItemScope.put("join_table", pagItemScopeTableName);
            joinPagItemScope.put("on", pagItemTableName + ".id = " + pagItemScopeTableName + "." + pagItemName + "_id");
            joinPagItemScope.put("join_sel", pagItemScopeTableName + ".scope_id AS scope_id");

            joinProjectItemProjectScope.put("join_table", projectScopeTableName);
            joinProjectItemProjectScope.put("on", pagItemScopeTableName + ".scope_id = " + projectScopeTableName + ".id");
            joinProjectItemProjectScope.put("join_sel", projectScopeTableName + ".id AS scope_id");
        }else {
            // is project item
            joinProjectItemProjectScope.put("join_table", projectScopeTableName);
            joinProjectItemProjectScope.put("on", projectItemTableName + ".scope_id = " + projectScopeTableName + ".id");
            joinProjectItemProjectScope.put("join_sel", projectScopeTableName + ".id AS scope_id");
        }

        Map<String, Object> joinScopeLocation = new HashMap<>();
        joinScopeLocation.put("join_table", projectLocationTableName + " AS l");
        joinScopeLocation.put("on", projectScopeTableName + ".location_id = l.id");
        joinScopeLocation.put("join_sel", "l.id AS location_id");

        Map<String, Object> joinLocationGroup = new HashMap<>();
        joinLocationGroup.put("join_table", projectLocationGroupTableName + " AS lg");
        joinLocationGroup.put("on", projectScopeTableName + ".location_group_id = lg.id");
        joinLocationGroup.put("join_sel", "lg.id AS location_group_id");

        Map<String, Object> joinBuilding = new HashMap<>();
        joinBuilding.put("join_table", projectBuildingTableName + " AS b");
        joinBuilding.put("on", projectScopeTableName + ".building_id = b.id");
        joinBuilding.put("join_sel", "b.id AS building_id");

        Map<String, Object> joinSite = new HashMap<>();
        joinSite.put("join_table", projectSiteTableName + " AS s");
        joinSite.put("on", projectScopeTableName + ".site_id = s.id");
        joinSite.put("join_sel", "s.id AS site_id");

        Map<String, Object> joinSiteGroup = new HashMap<>();
        joinSiteGroup.put("join_table", projectSiteGroupTableName + " AS sg");
        joinSiteGroup.put("on", projectScopeTableName + ".site_group_id = sg.id");
        joinSiteGroup.put("join_sel", "lg.id AS site_group_id");

        List<Map<String, Object>> joinOnList = new ArrayList<>();
        if(!joinPagItemScope.isEmpty()) {
            joinOnList.add(joinPagItemScope);
        }
        if(!joinProjectItemProjectScope.isEmpty()) {
            joinOnList.add(joinProjectItemProjectScope);
        }
        joinOnList.add(joinScopeLocation);
        joinOnList.add(joinLocationGroup);
        joinOnList.add(joinBuilding);
        joinOnList.add(joinSite);
        joinOnList.add(joinSiteGroup);

        Map<String, Object> result = new HashMap<>();
        result.put("join_on_list", joinOnList);
        return result;
    }

    public Map<String, Object> getFlexiItemScopeJoin(Map<String, Object> request) {
        logger.info("getFlexiItemScopeJoin()");

        Map<String, Object> joinOnListResult = getFlexiItemScopeJoinOnList(request);
        if(joinOnListResult.get("error") != null) {
            return joinOnListResult;
        }

        List<Map<String, String>> joinOnList = (List<Map<String, String>>) joinOnListResult.get("join_on_list");

        String joinType = (String) request.getOrDefault("join_type", "LEFT JOIN");
        StringBuilder sql = new StringBuilder();
        for(Map<String, String> joinOn : joinOnList) {
            if(joinOn.get("join_table") == null || joinOn.get("on") == null) {
                return Map.of("error", "Missing join table or on condition");
            }
            sql.append(" ").append(joinType).append(" ").append(joinOn.get("join_table")).append(" ON ").append(joinOn.get("on"));
            if(joinOn.get("or") != null) {
                sql.append(joinOn.get("or"));
            }
        }
        return Map.of("join_sql", sql.toString());
    }
}
