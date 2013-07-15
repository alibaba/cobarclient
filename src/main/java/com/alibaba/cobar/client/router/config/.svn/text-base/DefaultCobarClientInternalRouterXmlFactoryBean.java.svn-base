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
import java.util.ArrayList;
import java.util.List;

import org.springframework.core.io.Resource;

import com.alibaba.cobar.client.router.config.vo.InternalRule;
import com.alibaba.cobar.client.router.config.vo.InternalRules;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.thoughtworks.xstream.XStream;
/**
 * 
 * {@link DefaultCobarClientInternalRouterXmlFactoryBean} will load rule definitions from external xml configuration files.<br>
 * if you want to directly define rules in spring's IoC Container, see {@link StaticCobarClientInternalRouterFactoryBean}.
 * @author fujohnwang
 * @see    StaticCobarClientInternalRouterFactoryBean
 */
public class DefaultCobarClientInternalRouterXmlFactoryBean extends
        AbstractCobarClientInternalRouterFactoryBean {

    private Resource   configLocation;
    private Resource[] configLocations;

    public Resource getConfigLocation() {
        return configLocation;
    }

    public void setConfigLocation(Resource configLocation) {
        this.configLocation = configLocation;
    }

    public Resource[] getConfigLocations() {
        return configLocations;
    }

    public void setConfigLocations(Resource[] configLocations) {
        this.configLocations = configLocations;
    }

    @Override
    protected List<InternalRule> loadRulesFromExternal() throws IOException {
        XStream xstream = new XStream();
        xstream.alias("rules", InternalRules.class);
        xstream.alias("rule", InternalRule.class);
        xstream.addImplicitCollection(InternalRules.class, "rules");
        xstream.useAttributeFor(InternalRule.class, "merger");

        List<InternalRule> rules = new ArrayList<InternalRule>();

        if (getConfigLocation() != null) {
            InternalRules internalRules = (InternalRules) xstream.fromXML(getConfigLocation()
                    .getInputStream());
            if (!CollectionUtils.isEmpty(internalRules.getRules())) {
                rules.addAll(internalRules.getRules());
            }
        }
        if (getConfigLocations() != null && getConfigLocations().length > 0) {
            for (Resource resource : getConfigLocations()) {
                InternalRules internalRules = (InternalRules) xstream.fromXML(resource
                        .getInputStream());
                if (!CollectionUtils.isEmpty(internalRules.getRules())) {
                    rules.addAll(internalRules.getRules());
                }
            }
        }

        return rules;
    }

}
