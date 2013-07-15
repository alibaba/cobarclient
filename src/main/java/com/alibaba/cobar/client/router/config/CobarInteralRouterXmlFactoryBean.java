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
 package com.alibaba.cobar.client.router.config;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.springframework.core.io.Resource;

import com.alibaba.cobar.client.router.CobarClientInternalRouter;
import com.alibaba.cobar.client.router.config.vo.InternalRule;
import com.alibaba.cobar.client.router.config.vo.InternalRules;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisNamespaceRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisNamespaceShardingRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisSqlActionRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisSqlActionShardingRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;
import com.thoughtworks.xstream.XStream;

public class CobarInteralRouterXmlFactoryBean extends
        AbstractCobarInternalRouterConfigurationFactoryBean {

    @Override
    protected void assembleRulesForRouter(
                                          CobarClientInternalRouter router,
                                          Resource configLocation,
                                          Set<IRoutingRule<IBatisRoutingFact, List<String>>> sqlActionShardingRules,
                                          Set<IRoutingRule<IBatisRoutingFact, List<String>>> sqlActionRules,
                                          Set<IRoutingRule<IBatisRoutingFact, List<String>>> namespaceShardingRules,
                                          Set<IRoutingRule<IBatisRoutingFact, List<String>>> namespaceRules)
            throws IOException {
        XStream xstream = new XStream();
        xstream.alias("rules", InternalRules.class);
        xstream.alias("rule", InternalRule.class);
        xstream.addImplicitCollection(InternalRules.class, "rules");
        xstream.useAttributeFor(InternalRule.class, "merger");

        InternalRules internalRules = (InternalRules) xstream.fromXML(configLocation
                .getInputStream());
        List<InternalRule> rules = internalRules.getRules();
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
                if (StringUtils.isEmpty(shardingExpression)) {
                    namespaceRules.add(new IBatisNamespaceRule(namespace, destinations));
                } else {
                    IBatisNamespaceShardingRule insr = new IBatisNamespaceShardingRule(namespace,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(getFunctionsMap())) {
                        insr.setFunctionMap(getFunctionsMap());
                    }
                    namespaceShardingRules.add(insr);
                }
            }
            if (StringUtils.isNotEmpty(sqlAction)) {
                if (StringUtils.isEmpty(shardingExpression)) {
                    sqlActionRules.add(new IBatisSqlActionRule(sqlAction, destinations));
                } else {
                    IBatisSqlActionShardingRule issr = new IBatisSqlActionShardingRule(sqlAction,
                            destinations, shardingExpression);
                    if (MapUtils.isNotEmpty(getFunctionsMap())) {
                        issr.setFunctionMap(getFunctionsMap());
                    }
                    sqlActionShardingRules.add(issr);
                }
            }
        }

    }

}
