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

import com.alibaba.cobar.client.router.ICobarRouter;

/**
 * a rule acts in a "when-then" behavior, in our case, when the fact
 * {@link #isDefinedAt(Object)} or matches, then we will return action result.
 * the {@link ICobarRouter} will decide how to use these action result.
 * 
 * @author fujohnwang
 * @since 1.0
 */
public interface IRoutingRule<F, T> {
    /**
     * @param <F>, the type of the routing fact
     * @param routeFact, the fact to check against
     * @return
     */
    boolean isDefinedAt(F routingFact);

    /**
     * if a update or delete will involve multiple data sources, we have to
     * return a group of data sources to use.<br>
     * for rules the matches only one data source, return a set with size==1.<br>
     * 
     * @return
     */
    T action();
}
