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

import org.springframework.core.io.Resource;

import com.alibaba.cobar.client.router.CobarClientInternalRouter;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;

/**
 * read rules from decision table which is stored with excel file.<br>
 * except for the 1st row as title, other each rows will be rule definitions.<br>
 * it seems like:
 * 
 * <pre>
 * --------------------------------------------------------------------
 * namespace   | sqlaction         |  shardingExpression|   shards    |
 * --------------------------------------------------------------------
 * com...Offer |                   | memberId < 10000   | shardOne,   |
 * --------------------------------------------------------------------
 *             |com...Offer.create | memberId > 1000000 | shardOne,   |
 * --------------------------------------------------------------------
 * </pre>
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class CobarInternalRouterXlsRuleFactoryBean extends
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
        // TODO Auto-generated method stub

    }

}
