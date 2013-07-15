package com.alibaba.cobar.client.router.rules;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Tweet;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisSqlActionShardingRule;
import com.alibaba.cobar.client.router.rules.support.ModFunction;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

/**
 * TODO Comment of IBatisSqlActionShardingRuleTest
 * 
 * @author fujohnwang
 * @see {@link IBatisNamespaceShardingRuleTest} for more test scenarios.
 */
@Test
public class IBatisSqlActionShardingRuleTest{
    // almost copied from IBatisNamespaceShardingRuleTest, although a same top class is better.
    public static final String          DEFAULT_TYPE_PATTEN      = "com.alibaba.cobar.client.entity.Tweet.create";
    public static final String          DEFAULT_SHARDING_PATTERN = "id>=10000 and id < 20000";
    public static final String[]        DEFAULT_SHARDS           = { "shard1", "shard2" };

    private IBatisSqlActionShardingRule rule;

    @BeforeMethod
    protected void setUp() throws Exception {
        rule = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2",
                DEFAULT_SHARDING_PATTERN);
    }
    @AfterMethod
    protected void tearDown() throws Exception {
        rule = null;
    }

    public void testSqlActionShardingRuleConstructionAbnormally() {
        try {
            rule = new IBatisSqlActionShardingRule(null, "shard1,shard2", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new IBatisSqlActionShardingRule("", "shard1,shard2", DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN, "",
                    DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN, null,
                    DEFAULT_SHARDING_PATTERN);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2", null);
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }

        try {
            rule = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN, "shard1,shard2", "");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testSqlActionShardingRulePatternMatchingNormally() {
        Tweet t = new Tweet();
        t.setId(15000L);
        t.setTweet("anything");

        IBatisRoutingFact fact = new IBatisRoutingFact(
                "com.alibaba.cobar.client.entity.Tweet.create", t);
        assertTrue(rule.isDefinedAt(fact));
        List<String> shards = rule.action();
        assertTrue(CollectionUtils.isNotEmpty(shards));
        assertEquals(2, shards.size());
        for (String shard : shards) {
            assertTrue(ArrayUtils.contains(DEFAULT_SHARDS, shard));
        }

        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Tweet.update", t);
        assertFalse(rule.isDefinedAt(fact));

        t.setId(20000L);
        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Tweet.create", t);
        assertFalse(rule.isDefinedAt(fact));

        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Tweet.create", null);
        assertFalse(rule.isDefinedAt(fact));

        fact = new IBatisRoutingFact("com.alibaba.cobar.client.entity.Tweet.create", new Object());
        assertFalse(rule.isDefinedAt(fact));
    }

    public void testSqlActionShardingRulePatternMatchingAbnormally() {
        try {
            rule.setActionPatternSeparator(null);
            fail();
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

    public void testSqlActionShardingRuleWithCustomFunctions() {
        IBatisSqlActionShardingRule r = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN,
                "shard1,shard2", "mod.apply(id)==3");
        Map<String, Object> functions = new HashMap<String, Object>();
        functions.put("mod", new ModFunction(18L));
        r.setFunctionMap(functions);
        
        Tweet t = new Tweet();
        t.setId(21L);
        t.setTweet("anything");
        IBatisRoutingFact fact = new IBatisRoutingFact(
                "com.alibaba.cobar.client.entity.Tweet.create", t);
        assertTrue(r.isDefinedAt(fact));
    }
    
    public void testSqlActionShardingRuleWithSimpleContextObjectType(){
        IBatisSqlActionShardingRule r = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN,
                "shard1", "$ROOT.startsWith(\"J\")");
        IBatisRoutingFact fact = new IBatisRoutingFact(
                "com.alibaba.cobar.client.entity.Tweet.create", "Jack");
        assertTrue(r.isDefinedAt(fact));
        
        r = new IBatisSqlActionShardingRule(DEFAULT_TYPE_PATTEN,
                "shard1", "startsWith(\"J\")");
        assertTrue(r.isDefinedAt(fact));
        
        fact = new IBatisRoutingFact(
                "com.alibaba.cobar.client.entity.Tweet.create", "Amanda");
        assertFalse(r.isDefinedAt(fact));
        
        
    }
}
