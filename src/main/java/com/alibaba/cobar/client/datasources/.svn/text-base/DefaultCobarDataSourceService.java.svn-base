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
 package com.alibaba.cobar.client.datasources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang.Validate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import com.alibaba.cobar.client.datasources.ha.IHADataSourceCreator;
import com.alibaba.cobar.client.datasources.ha.NonHADataSourceCreator;
import com.alibaba.cobar.client.datasources.ha.support.IDataSourcePostProcessor;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

/**
 * StrongRefDataSourceLocator is mainly responsible for assembling data sources
 * mapping relationship as per data source definitions in spring container.
 * 
 * @author fujohnwang
 */
public class DefaultCobarDataSourceService implements ICobarDataSourceService, InitializingBean {
    private Set<CobarDataSourceDescriptor> dataSourceDescriptors   = new HashSet<CobarDataSourceDescriptor>();
    private List<IDataSourcePostProcessor> dataSourcePostProcessor = new ArrayList<IDataSourcePostProcessor>();
    private IHADataSourceCreator           haDataSourceCreator;
    private Map<String, DataSource>        dataSources             = new HashMap<String, DataSource>();

    public Map<String, DataSource> getDataSources() {
        return dataSources;
    }

    public void afterPropertiesSet() throws Exception {
        if (getHaDataSourceCreator() == null) {
            setHaDataSourceCreator(new NonHADataSourceCreator());
        }
        if (CollectionUtils.isEmpty(dataSourceDescriptors)) {
            return;
        }

        for (CobarDataSourceDescriptor descriptor : getDataSourceDescriptors()) {
            Validate.notEmpty(descriptor.getIdentity());
            Validate.notNull(descriptor.getTargetDataSource());

            DataSource dataSourceToUse = descriptor.getTargetDataSource();

            if (descriptor.getStandbyDataSource() != null) {
                dataSourceToUse = getHaDataSourceCreator().createHADataSource(descriptor);
                if (CollectionUtils.isNotEmpty(dataSourcePostProcessor)) {
                    for (IDataSourcePostProcessor pp : dataSourcePostProcessor) {
                        dataSourceToUse = pp.postProcess(dataSourceToUse);
                    }
                }
            }

            dataSources.put(descriptor.getIdentity(), new LazyConnectionDataSourceProxy(
                    dataSourceToUse));
        }
    }

    public void setDataSourceDescriptors(Set<CobarDataSourceDescriptor> dataSourceDescriptors) {
        this.dataSourceDescriptors = dataSourceDescriptors;
    }

    public Set<CobarDataSourceDescriptor> getDataSourceDescriptors() {
        return dataSourceDescriptors;
    }

    public void setHaDataSourceCreator(IHADataSourceCreator haDataSourceCreator) {
        this.haDataSourceCreator = haDataSourceCreator;
    }

    public IHADataSourceCreator getHaDataSourceCreator() {
        return haDataSourceCreator;
    }

}
