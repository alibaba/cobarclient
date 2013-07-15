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
 package com.alibaba.cobar.client.router.rules;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

/**
 * horizontal partitioning oriented rule that matches against entity/table type and attribute values.<br>
 * 
 * @author fujohnwang
 *
 * @param <F>
 * @param <T>
 */
public abstract class AbstractEntityAttributeRule<F, T> extends AbstractEntityTypeRule<F, T> {
    private String attributePattern;
    
    public AbstractEntityAttributeRule(String typePattern, String action)
    {
        super(typePattern, action);
    }
    
    public AbstractEntityAttributeRule(String typePattern, String action, String attributePattern)
    {
        super(typePattern, action);
        Validate.notEmpty(StringUtils.trimToEmpty(attributePattern));
        
        this.attributePattern = attributePattern;
    }

    public String getAttributePattern() {
        return attributePattern;
    }

    public void setAttributePattern(String attributePattern) {
        Validate.notEmpty(StringUtils.trimToEmpty(attributePattern));
        this.attributePattern = attributePattern;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((attributePattern == null) ? 0 : attributePattern.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractEntityAttributeRule other = (AbstractEntityAttributeRule) obj;
        if (attributePattern == null) {
            if (other.attributePattern != null)
                return false;
        } else if (!attributePattern.equals(other.attributePattern))
            return false;
        return true;
    }
    
}
