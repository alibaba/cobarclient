package com.alibaba.cobarclient.route;

import com.alibaba.cobarclient.Shard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class SimpleRouter implements Router {

    protected Logger logger = Logger.getLogger("SimpleRouter");

    private Map<String, RouteGroup> routes = new HashMap<String, RouteGroup>();

    private Set<Shard> EMPTY_SHARD_SET = new HashSet<Shard>();

    public SimpleRouter(Set<Route> routeSet) {
        if (!(routeSet == null || routeSet.isEmpty())) {
            for (Route r : routeSet) {
                if (!routes.containsKey(r.getSqlmap())) routes.put(r.getSqlmap(), new RouteGroup());
                if (r.getExpression() == null)
                    routes.get(r.getSqlmap()).setFallbackRoute(r);
                else
                    routes.get(r.getSqlmap()).getSpecificRoutes().add(r);
            }
        }
    }

    public Set<Shard> route(String action, Object argument) {
        Route resultRoute = findRoute(action, argument);
        if (resultRoute == null) {
            if (action != null) {
                String namespace = action.substring(0, action.lastIndexOf("."));
                resultRoute = findRoute(namespace, argument);
            }
        }
        if (resultRoute == null) {
            return EMPTY_SHARD_SET;
        } else {
            return resultRoute.getShards();
        }
    }


    protected Route findRoute(String action, Object argument) {
        if (routes.containsKey(action)) {
            RouteGroup routeGroup = routes.get(action);
            for (Route route : routeGroup.getSpecificRoutes()) {
                if (route.apply(action, argument)) {
                    return route;
                }
            }
            if (routeGroup.getFallbackRoute() != null && routeGroup.getFallbackRoute().apply(action, argument))
                return routeGroup.getFallbackRoute();
        }
        return null;
    }
}
