package com.alibaba.cobarclient.route;

import com.alibaba.cobarclient.Shard;

import java.util.Set;

public interface Router {
    Set<Shard> route(String action, Object argument);
}
