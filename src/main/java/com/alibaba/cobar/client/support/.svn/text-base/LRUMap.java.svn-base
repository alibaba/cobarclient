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
 package com.alibaba.cobar.client.support;

import java.util.LinkedHashMap;

public class LRUMap<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = -3700466745992492679L;

    private int               coreSize;

    public LRUMap(int coreSize) {
        super(coreSize + 1, 1.1f, true);
        this.coreSize = coreSize;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
        return size() > coreSize;
    }
}
