package com.alibaba.cobarclient.route;

import com.alibaba.cobarclient.Shard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StaticRoutesRouter implements Router {
    private Map<String, Set<Route>> routes = new HashMap<String, Set<Route>>();

    private Set<Shard> EMPTY_SHARD_SET = new HashSet<Shard>();

    public StaticRoutesRouter(Set<Route> routeSet) {
        if (!(routeSet == null || routeSet.isEmpty())) {
            for (Route r : routeSet) {
                if (!routes.containsKey(r.getSqlmap())) routes.put(r.getSqlmap(), new HashSet<Route>());
                routes.get(r.getSqlmap()).add(r);
            }
        }
    }

    public Set<Shard> route(String action, Object argument) {
        Route resultRoute = findRoute(action, argument);
        if (resultRoute == null) {
            String namespace = action.substring(0, action.lastIndexOf("."));
            resultRoute = findRoute(namespace, argument);
        }
        if (resultRoute == null) {
            return EMPTY_SHARD_SET;
        } else {
            return resultRoute.getShards();
        }
    }


    protected Route findRoute(String action, Object argument) {
        if (routes.containsKey(action)) {
            for (Route route : routes.get(action)) {
                if (route.apply(action, argument)) {
                    return route;
                }
            }
        }
        return null;
    }
}
