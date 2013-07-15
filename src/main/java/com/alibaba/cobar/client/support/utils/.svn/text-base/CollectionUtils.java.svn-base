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
 package com.alibaba.cobar.client.support.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectionUtils {
    @SuppressWarnings("rawtypes")
    public static Collection select(Collection inputCollection, Predicate predicate) {
        List answer = new ArrayList(inputCollection.size());
        select(inputCollection, predicate, answer);
        return answer;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void select(Collection inputCollection, Predicate predicate,
                              Collection outputCollection) {
        if (inputCollection != null && predicate != null) {
            for (Iterator iter = inputCollection.iterator(); iter.hasNext();) {
                Object item = iter.next();
                if (predicate.evaluate(item)) {
                    outputCollection.add(item);
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static boolean isEmpty(Collection coll) {
        return (coll == null || coll.isEmpty());
    }

    @SuppressWarnings("rawtypes")
    public static boolean isNotEmpty(Collection coll) {
        return !CollectionUtils.isEmpty(coll);
    }
}
