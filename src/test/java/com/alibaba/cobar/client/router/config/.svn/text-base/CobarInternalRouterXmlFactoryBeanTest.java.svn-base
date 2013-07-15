package com.alibaba.cobar.client.router.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.router.CobarClientInternalRouter;
import com.alibaba.cobar.client.router.rules.IRoutingRule;
import com.alibaba.cobar.client.router.rules.ibatis.AbstractIBatisOrientedRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisNamespaceShardingRule;
import com.alibaba.cobar.client.router.rules.ibatis.IBatisSqlActionShardingRule;
import com.alibaba.cobar.client.router.support.IBatisRoutingFact;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.utils.MapUtils;

@Test
public class CobarInternalRouterXmlFactoryBeanTest {
    CobarInteralRouterXmlFactoryBean factory;

    @BeforeMethod
    protected void setUp() throws Exception {
        factory = new CobarInteralRouterXmlFactoryBean();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        factory = null;
    }

    public void testAssemblingRulesNormally() throws Exception {
        factory.setConfigLocation(new ClassPathResource(
                "com/alibaba/cobar/client/router/config/normal_rule_fixture.xml"));
        factory.afterPropertiesSet();
        CobarClientInternalRouter router = (CobarClientInternalRouter) factory.getObject();
        List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> rules = router.getRuleSequences();
        assertTrue(CollectionUtils.isNotEmpty(rules));
        assertEquals(4, rules.size());
    }

    public void testAssemblingRulesWithoutConfiguringShards() {
        factory.setConfigLocation(new ClassPathResource(
                "com/alibaba/cobar/client/router/config/abnormal_rule_fixture1.xml"));
        try {
            factory.afterPropertiesSet();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("destination shards must be given explicitly.", e.getMessage());
        }
    }

    public void testAssemblingRulesWithoutConfiguringNamspaceOrSqlmap() {
        factory.setConfigLocation(new ClassPathResource(
                "com/alibaba/cobar/client/router/config/abnormal_rule_fixture2.xml"));
        try {
            factory.afterPropertiesSet();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("at least one of 'namespace' or 'sqlAction' must be given.", e
                    .getMessage());
        }
    }

    public void testAssemblingRulesWithConfiguringBothNamspaceAndSqlmap() {
        factory.setConfigLocation(new ClassPathResource(
                "com/alibaba/cobar/client/router/config/abnormal_rule_fixture3.xml"));
        try {
            factory.afterPropertiesSet();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals(
                    "'namespace' and 'sqlAction' are alternatives, can't guess which one to use if both of them are provided.",
                    e.getMessage());
        }
    }

    public void testAssemblingRulesWithInvalidFormatConfiguration() {
        factory.setConfigLocation(new ClassPathResource(
                "com/alibaba/cobar/client/router/config/abnormal_rule_fixture4.xml"));
        try {
            factory.afterPropertiesSet();
            fail();
        } catch (Exception e) {
            // pass
        }
    }

    public void testAssemblingRulesWithNonExistenceConfiguration() {
        factory.setConfigLocation(new ClassPathResource(
                "com/alibaba/cobar/client/router/config/abnormal_fixture.xml"));
        try {
            factory.afterPropertiesSet();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof FileNotFoundException);
        }
    }

    /**
     * load two set of rules with same rule entries, same rule entry should be
     * merge to one.
     * 
     * @throws Exception
     */
    public void testAssemblingRulesWithMultipleSameConfigurations() throws Exception {
        Map<String, Object> functions = new HashMap<String, Object>();
        functions.put("mock_function", new Object());
        factory.setFunctionsMap(functions);

        factory.setConfigLocations(new Resource[] {
                new ClassPathResource(
                        "com/alibaba/cobar/client/router/config/normal_rule_fixture.xml"),
                new ClassPathResource(
                        "com/alibaba/cobar/client/router/config/normal_rule_fixture2.xml") });
        factory.afterPropertiesSet();
        CobarClientInternalRouter router = (CobarClientInternalRouter) factory.getObject();
        List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> rules = router.getRuleSequences();
        assertTrue(CollectionUtils.isNotEmpty(rules));
        assertEquals(4, rules.size());

        for (Set<IRoutingRule<IBatisRoutingFact, List<String>>> set : rules) {
            assertEquals(1, set.size());
            IRoutingRule<IBatisRoutingFact, List<String>> r = set.iterator().next();
            if(r instanceof IBatisNamespaceShardingRule || r instanceof IBatisSqlActionShardingRule)
            {
                Map<String, Object> funcMap = ((AbstractIBatisOrientedRule)r).getFunctionMap();
                assertTrue(MapUtils.isNotEmpty(funcMap));
                assertEquals("mock_function", funcMap.keySet().iterator().next());
            }
        }
    }

    public void testAssemblingRulesWithMultiplePartialSameConfiguration() throws Exception {
        factory.setConfigLocations(new Resource[] {
                new ClassPathResource(
                        "com/alibaba/cobar/client/router/config/normal_rule_fixture.xml"),
                new ClassPathResource(
                        "com/alibaba/cobar/client/router/config/normal_rule_fixture3.xml") });
        factory.afterPropertiesSet();
        CobarClientInternalRouter router = (CobarClientInternalRouter) factory.getObject();
        List<Set<IRoutingRule<IBatisRoutingFact, List<String>>>> rules = router.getRuleSequences();
        assertTrue(CollectionUtils.isNotEmpty(rules));
        assertEquals(4, rules.size());

        assertEquals(1, rules.get(0).size());
        assertEquals(1, rules.get(1).size());
        assertEquals(2, rules.get(2).size());
        assertEquals(1, rules.get(3).size());
    }
}
