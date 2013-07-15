/**
 * Copyright (C) 2010 Alibaba.com Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.alibaba.cobar.client.router.aspects;

import java.util.Arrays;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.alibaba.cobar.client.router.DefaultCobarClientInternalRouter;
import com.alibaba.cobar.client.router.ICobarRouter;
import com.alibaba.cobar.client.router.config.AbstractCobarClientInternalRouterFactoryBean;
import com.alibaba.cobar.client.router.config.StaticCobarClientInternalRouterFactoryBean;
import com.alibaba.cobar.client.support.LRUMap;

/**
 * An advice that will provide cache service for {@link ICobarRouter} to improve
 * the routing performance if necessary.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 * @see {@link ICobarRouter}
 * @see {@link AbstractCobarClientInternalRouterFactoryBean}
 * @see {@link DefaultCobarClientInternalRouter}
 * @see {@link StaticCobarClientInternalRouterFactoryBean}
 */
public class RoutingResultCacheAspect implements MethodInterceptor {

    private LRUMap internalCache = new LRUMap(1000);

    public Object invoke(MethodInvocation invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        if (args.length != 1) {
            throw new IllegalArgumentException("unexpected argument status on method:"
                    + invocation.getMethod() + ", args:" + Arrays.toString(args));
        }

        synchronized (internalCache) {
            if (internalCache.containsKey(args[0])) {
                return internalCache.get(args[0]);
            }
        }

        Object result = null;
        try {
            result = invocation.proceed();
        } finally {
            synchronized (internalCache) {
                internalCache.put(args[0], result);
            }
        }

        return result;
    }

    public void setInternalCache(LRUMap internalCache) {
        if (internalCache == null) {
            throw new IllegalArgumentException("Null Cache Map is not allowed.");
        }
        this.internalCache = internalCache;
    }

    public LRUMap getInternalCache() {
        return internalCache;
    }

}
