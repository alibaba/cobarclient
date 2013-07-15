package com.alibaba.cobar.client.router.rules;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.List;

import org.testng.annotations.Test;

import com.alibaba.cobar.client.router.rules.ibatis.IBatisNamespaceRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test
public class IBatisNamespaceRuleTest{
    public void testNamespaceRuleNormally() {
        IBatisNamespaceRule rule = new IBatisNamespaceRule("com.alibaba.cobar.client.entity.Tweet",
                "p1, p2");
        List<String> shardIds = rule.action();
        assertNotNull(shardIds);
        assertEquals(2, shardIds.size());

        IBatisRoutingFact fact = new IBatisRoutingFact(
                "com.alibaba.cobar.client.entity.Tweet.update", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Tweet.delete", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Twet.delete", null);
        assertFalse(rule.isDefinedAt(fact));
    }

    public void testNamespaceRuleNormallyWithCustomActionPatternSeparator() {
        IBatisNamespaceRule rule = new IBatisNamespaceRule("com.alibaba.cobar.client.entity.Tweet",
                "p1, p2");
        rule.setActionPatternSeparator(";");
        List<String> shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(1, shards.size());

        rule = new IBatisNamespaceRule("com.alibaba.cobar.client.entity.Tweet", "p1; p2");
        rule.setActionPatternSeparator(";");
        shards = null;
        shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(2, shards.size());

        IBatisRoutingFact fact = new IBatisRoutingFact(
                "com.alibaba.cobar.client.entity.Tweet.update", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Tweet.delete", null);
        assertTrue(rule.isDefinedAt(fact));
        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Twet.delete", null);
        assertFalse(rule.isDefinedAt(fact));
    }

    public void testNamespaceRuleAbnormally() {
        try {
            new IBatisNamespaceRule("", "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new IBatisNamespaceRule("", null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new IBatisNamespaceRule(null, "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            new IBatisNamespaceRule(null, null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        IBatisNamespaceRule rule = new IBatisNamespaceRule("com.alibaba.cobar.client.entity.Tweet",
                "p1, p2");
        try {
            rule.setActionPatternSeparator(null);
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule.isDefinedAt(null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }
}
