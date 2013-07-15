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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
/**
 * Vertical Partitioning Rule which maps iBatis's namespace in sqlmap file to entity/table type.<br>
 * 
 * @author fujohnwang
 * @since  1.0 
 */
public class IBatisNamespaceRule extends AbstractIBatisOrientedRule {
    
    public IBatisNamespaceRule(String pattern, String action) {
        super(pattern, action);
    }

    public boolean isDefinedAt(IBatisRoutingFact routingFact) {
        Validate.notNull(routingFact);
        String namespace = StringUtils.substringBeforeLast(routingFact.getAction(), ".");
        return StringUtils.equals(namespace, getTypePattern());
    }

    @Override
    public String toString() {
        return "IBatisNamespaceRule [getAction()=" + getAction() + ", getTypePattern()="
                + getTypePattern() + "]";
    }

}
