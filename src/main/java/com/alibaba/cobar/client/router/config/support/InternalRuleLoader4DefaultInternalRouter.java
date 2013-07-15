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
 package com.alibaba.cobar.client.router.config.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.alibaba.cobar.client.router.DefaultCobarClientInternalRouter;
import com.alibaba.cobar.client.router.config.vo.InternalRule;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisNamespaceRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisNamespaceShardingRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisSqlActionRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisSqlActionShardingRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;

public class InternalRuleLoader4DefaultInternalRouter {

    public void loadRulesAndEquipRouter(List<InternalRule> rules,
                                        DefaultCobarClientInternalRouter router,
                                        Map<String, Object> functionsMap) {
        if (CollectionUtils.isEmpty(rules)) {
            return;
        }

        for (InternalRule rule : rules) {
            String namespace = StringUtils.trimToEmpty(rule.getNamespace());
            String sqlAction = StringUtils.trimToEmpty(rule.getSqlmap());
            String shardingExpression = StringUtils.trimToEmpty(rule.getShardingExpression());
            String destinations = StringUtils.trimToEmpty(rule.getShards());

            Validate.notEmpty(destinations, "destination shards must be given explicitly.");

            if (StringUtils.isEmpty(namespace) && StringUtils.isEmpty(sqlAction)) {
                throw new IllegalArgumentException(
                        "at least one of 'namespace' or 'sqlAction' must be given.");
            }
            if (StringUtils.isNotEmpty(namespace) && StringUtils.isNotEmpty(sqlAction)) {
                throw new IllegalArgumentException(
                        "'namespace' and 'sqlAction' are alternatives, can't guess which one to use if both of them are provided.");
            }

            if (StringUtils.isNotEmpty(namespace)) {
                List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> ruleSequence = setUpRuleSequenceContainerIfNecessary(
                        router, namespace);

                if (StringUtils.isEmpty(shardingExpression)) {

                    ruleSequence.get(3).add(new IBatisNamespaceRule(namespace, destinations));
                } else {
                    IBatisNamespaceShardingRule insr = new IBatisNamespaceShardingRule(namespace,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(functionsMap)) {
                        insr.setFunctionMap(functionsMap);
                    }
                    ruleSequence.get(2).add(insr);
                }
            }
            if (StringUtils.isNotEmpty(sqlAction)) {
                List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> ruleSequence = setUpRuleSequenceContainerIfNecessary(
                        router, StringUtils.substringBeforeLast(sqlAction, "."));

                if (StringUtils.isEmpty(shardingExpression)) {
                    ruleSequence.get(1).add(new IBatisSqlActionRule(sqlAction, destinations));
                } else {
                    IBatisSqlActionShardingRule issr = new IBatisSqlActionShardingRule(sqlAction,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(functionsMap)) {
                        issr.setFunctionMap(functionsMap);
                    }
                    ruleSequence.get(0).add(issr);
                }
            }
        }
    }

    private List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> setUpRuleSequenceContainerIfNecessary(
                                                                                                           DefaultCobarClientInternalRouter routerToUse,
                                                                                                           String namespace) {
        List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> ruleSequence = routerToUse
                .getRulesGroupByNamespaces().get(namespace);
        if (CollectionUtils.isEmpty(ruleSequence)) {
            ruleSequence = new ArrayList<Set<IRoutingRule<IBatisRoutingFact, List<String>>>>();
            Set<IRoutingRule<IBatisRoutingFact, List<String>>> sqlActionShardingRules = new HashSet<IRoutingRule<IBatisRoutingFact, List<String>>>();
            Set<IRoutingRule<IBatisRoutingFact, List<String>>> sqlActionRules = new HashSet<IRoutingRule<IBatisRoutingFact, List<String>>>();
            Set<IRoutingRule<IBatisRoutingFact, List<String>>> namespaceShardingRules = new HashSet<IRoutingRule<IBatisRoutingFact, List<String>>>();
            Set<IRoutingRule<IBatisRoutingFact, List<String>>> namespaceRules = new HashSet<IRoutingRule<IBatisRoutingFact, List<String>>>();
            ruleSequence.add(sqlActionShardingRules);
            ruleSequence.add(sqlActionRules);
            ruleSequence.add(namespaceShardingRules);
            ruleSequence.add(namespaceRules);
            routerToUse.getRulesGroupByNamespaces().put(namespace, ruleSequence);
        }
        return ruleSequence;
    }
}
