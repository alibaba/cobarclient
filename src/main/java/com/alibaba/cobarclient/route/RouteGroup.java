package com.alibaba.cobarclient.route;

import java.util.HashSet;
import java.util.Set;

public class RouteGroup {

    private Route fallbackRoute = null;
    private Set<Route> specificRoutes = new HashSet<Route>();

    public RouteGroup() {
    }

    public RouteGroup(Route fallbackRoute, Set<Route> specificRoutes) {
        this.fallbackRoute = fallbackRoute;
        if (!(specificRoutes == null || specificRoutes.isEmpty())) {
            this.specificRoutes.addAll(specificRoutes);
        }
    }

    public Route getFallbackRoute() {
        return fallbackRoute;
    }

    public void setFallbackRoute(Route fallbackRoute) {
        this.fallbackRoute = fallbackRoute;
    }

    public Set<Route> getSpecificRoutes() {
        return specificRoutes;
    }

    public void setSpecificRoutes(Set<Route> specificRoutes) {
        this.specificRoutes = specificRoutes;
    }
}
