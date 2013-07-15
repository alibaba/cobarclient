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
 package com.alibaba.cobar.client.router.support;

import java.util.List;

import com.alibaba.cobar.client.merger.IMerger;

/**
 * @author fujohnwang
 * @since 1.0
 */
public class RoutingResult {
    private List<String>  resourceIdentities;
    private IMerger<?, ?> merger;

    public List<String> getResourceIdentities() {
        return resourceIdentities;
    }

    public void setResourceIdentities(List<String> resourceIdentities) {
        this.resourceIdentities = resourceIdentities;
    }

    public void setMerger(IMerger<?, ?> merger) {
        this.merger = merger;
    }

    public IMerger<?, ?> getMerger() {
        return merger;
    }

}
