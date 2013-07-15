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
 package com.alibaba.cobar.client.router.config.vo;

import java.util.ArrayList;
import java.util.List;

import com.thoughtworks.xstream.XStream;

public class InternalRules {

    private List<InternalRule> rules;

    public void setRules(List<InternalRule> rules) {
        this.rules = rules;
    }

    public List<InternalRule> getRules() {
        return rules;
    }

    public static void main(String[] args) {
        XStream xstream = new XStream();
        xstream.alias("rules", InternalRules.class);
        xstream.alias("rule", InternalRule.class);
        xstream.addImplicitCollection(InternalRules.class, "rules");
        xstream.useAttributeFor(InternalRule.class, "merger");

        InternalRules rules = new InternalRules();
        List<InternalRule> rList = new ArrayList<InternalRule>();
        InternalRule r1 = new InternalRule();
        r1.setNamespace("com.alibaba.cobar.client.entity.Follower");
        r1.setShards("partition1");
        rList.add(r1);
        InternalRule r2 = new InternalRule();
        r2.setSqlmap("com.alibaba.cobar.client.entity.Follower.create");
        r2.setShards("p1, p2");
        rList.add(r2);
        InternalRule r3 = new InternalRule();
        r3.setSqlmap("com.alibaba.cobar.client.entity.Follower.create");
        r3.setShardingExpression("id>10000 and id< 20000");
        r3.setShards("p1, p2");
        rList.add(r3);
        InternalRule r4 = new InternalRule();
        r4.setNamespace("com.alibaba.cobar.client.entity.Follower");
        r4.setShardingExpression("id>10000 and id< 20000");
        r4.setShards("p1, p2");
//        r4.setMerger(Runnable.class);
        rList.add(r4);
        rules.setRules(rList);

        String xml = xstream.toXML(rules);
        System.out.println(xml);
    }
}
