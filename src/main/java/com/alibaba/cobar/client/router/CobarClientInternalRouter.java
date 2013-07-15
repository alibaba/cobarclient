/**
 * Copyright 1999-2011 Alibaba Group
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
 package com.alibaba.cobar.client.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.router.support.RoutingResult;
import com.alibaba.cobar.client.support.LRUMap;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

/**
 * CobarInternalRouter is the default router that will be used in cobar client,
 * but it will not be the only one.<br>
 * if it can't meet the needs, we can provide other {@link ICobarRouter} like a
 * one that use rule engines.<br>
 * for now, CobarInternalRouter will hold 4 set of routing rules:
 * <ol>
 * <li>sqlActionShardingRules
 * <li>
 * <li>sqlActionRules
 * <li>
 * <li>namespaceShardingRules
 * <li>
 * <li>namespaceRules
 * <li>
 * </ol>
 * rules start with "sqlAction" are rules that will exactly match against the
 * sql-map action id in the routing fact, while rules start with "namesapce"
 * just match against the "namespace" part in the sql action id; we will match
 * these rules in sequence against the routing fact, each later rule will be
 * used as fall-back rule if former match fails.<br>
 * To enhance the rule matching performance, we add a LRU cache, you can decide
 * whether to use this cache by set the {@link #enableCache} property's value to
 * true or false.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class CobarClientInternalRouter implements ICobarRouter<IBatisRoutingFact> {

    private transient final Logger logger      = LoggerFactory.getLogger(CobarClientInternalRouter.class);

    private LRUMap                 localCache;
    private boolean                enableCache = false;

    public CobarClientInternalRouter(boolean enableCache) {
        this(enableCache, 10000);
    }

    public CobarClientInternalRouter(int cacheSize) {
        this(true, cacheSize);
    }

    public CobarClientInternalRouter(boolean enableCache, int cacheSize) {
        this.enableCache = enableCache;
        if (this.enableCache) {
            localCache = new LRUMap(cacheSize);
        }
    }

    private List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> ruleSequences          = new ArrayList<Set<IRoutingRule<IBatisRoutingFact, List<String>>>>();

    public RoutingResult doRoute(IBatisRoutingFact routingFact) throws RoutingException {
        if (enableCache) {
            synchronized (localCache) {
                if (localCache.containsKey(routingFact)) {
                    RoutingResult result = (RoutingResult) localCache.get(routingFact);
                    logger.info("return routing result:{} from cache for fact:{}", result, routingFact);
                    return result;
                }
            }
        }

        RoutingResult result = new RoutingResult();
        result.setResourceIdentities(new ArrayList<String>());

        IRoutingRule<IBatisRoutingFact, List<String>> ruleToUse = null;
        if (!CollectionUtils.isEmpty(getRuleSequences())) {
            for (Set<IRoutingRule<IBatisRoutingFact, List<String>>> ruleSet : getRuleSequences()) {
                ruleToUse = searchMatchedRuleAgainst(ruleSet, routingFact);
                if (ruleToUse != null) {
                    break;
                }
            }
        }

        if (ruleToUse != null) {
            logger.info("matched with rule:{} with fact:{}", ruleToUse, routingFact);
            result.getResourceIdentities().addAll(ruleToUse.action());
        } else {
            logger.info("No matched rule found for routing fact:{}", routingFact);
        }

        if (enableCache) {
            synchronized (localCache) {
                localCache.put(routingFact, result);
            }
        }

        return result;
    }

    private IRoutingRule<IBatisRoutingFact, List<String>> searchMatchedRuleAgainst(
                                                                                   Set<IRoutingRule<IBatisRoutingFact, List<String>>> rules,
                                                                                   IBatisRoutingFact routingFact) {
        if (CollectionUtils.isEmpty(rules)) {
            return null;
        }
        for (IRoutingRule<IBatisRoutingFact, List<String>> rule : rules) {
            if (rule.isDefinedAt(routingFact)) {
                return rule;
            }
        }
        return null;
    }

    public LRUMap getLocalCache() {
        return localCache;
    }

    public synchronized void clearLocalCache(){
        this.localCache.clear();
    }
    
    public boolean isEnableCache() {
        return enableCache;
    }

    public void setRuleSequences(List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> ruleSequences) {
        this.ruleSequences = ruleSequences;
    }

    public List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> getRuleSequences() {
        return ruleSequences;
    }

}
