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

import java.util.List;

import com.alibaba.cobar.client.router.config.vo.InternalRule;

/**
 * with StaticCobarClientInternalRouterFactoryBean, you can configure rules
 * directly in spring's IoC container.<br>
 * that's, declaring bean definitions of {@link InternalRule} directly as
 * dependency of {@link StaticCobarClientInternalRouterFactoryBean}.
 * 
 * @author fujohnwang
 * @see DefaultCobarClientInternalRouterXmlFactoryBean for another alternative
 *      which will load rule definitions from external xml.
 */
public class StaticCobarClientInternalRouterFactoryBean extends
        AbstractCobarClientInternalRouterFactoryBean {

    private List<InternalRule> rules;

    public void setRules(List<InternalRule> rules) {
        this.rules = rules;
    }

    public List<InternalRule> getRules() {
        return rules;
    }

    @Override
    protected List<InternalRule> loadRulesFromExternal() {
        return getRules();
    }

}
