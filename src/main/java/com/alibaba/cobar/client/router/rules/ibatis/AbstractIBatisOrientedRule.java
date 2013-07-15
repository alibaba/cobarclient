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
 package com.alibaba.cobar.client.router.rules.ibatis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.alibaba.cobar.client.router.rules.AbstractEntityAttributeRule;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
/**
 * super class for all of the {@link IRoutingRule} implementations that is oriented to be used to do routing with iBatis sqlmap.
 * 
 * @author fujohnwang
 * @since  1.0
 */
public abstract class AbstractIBatisOrientedRule extends AbstractEntityAttributeRule<IBatisRoutingFact, List<String>> {
    public static final String DEFAULT_DATASOURCE_IDENTITY_SEPARATOR = ",";
    
    private Map<String, Object> functionMap = new HashMap<String, Object>();

    private String actionPatternSeparator = DEFAULT_DATASOURCE_IDENTITY_SEPARATOR;
    
    private List<String>       dataSourceIds                         = new ArrayList<String>();

    public AbstractIBatisOrientedRule(String pattern, String action) {
        super(pattern, action);
    }

    public AbstractIBatisOrientedRule(String pattern, String action, String attributePattern) {
        super(pattern, action, attributePattern);
    }

    public synchronized List<String> action() {
        if(CollectionUtils.isEmpty(dataSourceIds))
        {
            List<String> ids = new ArrayList<String>();
            for (String id : StringUtils.split(getAction(), getActionPatternSeparator())) {
                ids.add(StringUtils.trimToEmpty(id));
            }
            setDataSourceIds(ids);
        }
        return dataSourceIds;
    }

    public void setDataSourceIds(List<String> dataSourceIds) {
        this.dataSourceIds = dataSourceIds;
    }

    public List<String> getDataSourceIds() {
        return dataSourceIds;
    }

    public void setActionPatternSeparator(String actionPatternSeparator) {
        Validate.notNull(actionPatternSeparator);
        this.actionPatternSeparator = actionPatternSeparator;
    }

    public String getActionPatternSeparator() {
        return actionPatternSeparator;
    }

    public void setFunctionMap(Map<String, Object> functionMap) {
        this.functionMap = functionMap;
    }

    public Map<String, Object> getFunctionMap() {
        return functionMap;
    }

}
