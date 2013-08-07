package com.alibaba.cobarclient.route;

import com.alibaba.cobarclient.Shard;
import com.alibaba.cobarclient.expr.MVELExpression;
import com.google.common.collect.Sets;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.*;

@Test
public class SimpleRouterTest {
    public static DataSource dataSource1 = new SimpleDriverDataSource();
    public static DataSource dataSource2 = new SimpleDriverDataSource();
    public static Shard shard1 = new Shard("shard1", dataSource1);
    public static Shard shard2 = new Shard("shard1", dataSource2);
    public static String SQLMAP1 = "com.alibaba.domain.Offer.insert";
    public static String SQLMAP2 = "com.alibaba.domain.Offer";
    public static String SQLMAP3 = "com.alibaba.domain.Customer.find";
    public static String SQLMAP4 = "com.alibaba.domain.Customer";
    public static Route route1 = new Route(SQLMAP1, new MVELExpression("id==\"shard1\""), Sets.newHashSet(shard1));
    public static Route route2 = new Route(SQLMAP1, new MVELExpression("id==\"shard2\""), Sets.newHashSet(shard2));
    public static Route route12 = new Route(SQLMAP1, null, Sets.newHashSet(shard1, shard2));
    public static Route route3 = new Route(SQLMAP2, new MVELExpression("id==\"shard1\""), Sets.newHashSet(shard1));
    public static Route route4 = new Route(SQLMAP2, new MVELExpression("id==\"shard2\""), Sets.newHashSet(shard2));
    public static Route route34 = new Route(SQLMAP2, null, Sets.newHashSet(shard1, shard2));
    public static Router router = new SimpleRouter(Sets.newHashSet(route34, route3, route4, route1, route2, route12));
    public static Route route5a = new Route(SQLMAP4, new MVELExpression("id <= 1000"), Sets.newHashSet(shard1));
    public static Route route5b = new Route(SQLMAP4, new MVELExpression("id > 1000"), Sets.newHashSet(shard2));
    public static Router simpleRouter = new SimpleRouter(Sets.newHashSet(route5a, route5b));

    @Test
    public void testSimpleRouterWithSqlmapAndExpressionRoute() {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("id", "shard1");
        Set<Shard> resultSet = router.route(SQLMAP1, ctx);
        assertEquals(1, resultSet.size());
        Shard resultShard = resultSet.iterator().next();
        assertEquals(resultShard.getId(), shard1.getId());
        assertNotNull(resultShard.getDataSource());

        resultSet = router.route(null, null);
        assertTrue(resultSet != null);
        assertTrue(resultSet.isEmpty());

        ctx.put("id", "shard2");
        resultSet = router.route(SQLMAP1, ctx);
        assertTrue(resultSet.size() == 1);
        Shard shard = resultSet.iterator().next();
        assertEquals(shard.getId(), shard2.getId());
        assertNotNull(shard.getDataSource());

        ctx.put("id", "shard1");
        resultSet = router.route(SQLMAP2, ctx);
        assertTrue(resultSet.size() == 1);
        shard = resultSet.iterator().next();
        assertEquals(shard.getId(), shard1.getId());

        ctx.put("id", "shard2");
        resultSet = router.route(SQLMAP2, ctx);
        assertTrue(resultSet.size() == 1);
        shard = resultSet.iterator().next();
        assertEquals(shard.getId(), shard2.getId());

    }

    @Test
    public void testSimpleRouterWithSqlmapRouteOnly() {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("id", "shard3");

        Set<Shard> resultSet = router.route(SQLMAP1, ctx);
        assertTrue(resultSet != null && resultSet.size() == 2);
    }

    @Test
    public void testSimpleRouterWithNoRouteMatch() {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("id", "shard1");

        Set<Shard> shards = router.route(SQLMAP3, ctx);
        assertTrue(shards.isEmpty());
    }

    @Test
    public void testSimpleRouterRunWithConcurrent() {
        Set<Shard> shards = router.route(SQLMAP2, null);
        assertEquals(shards.size(), 2);
    }

    @Test
    public void testNamespaceShardingRouter() {
        Map<String, Object> ctx = new HashMap<String, Object>();
        ctx.put("id", Integer.valueOf(100));

        Set<Shard> shards = simpleRouter.route(SQLMAP4, ctx);
        assertEquals(shards.size(), 1);
        Shard shard = shards.iterator().next();
        assertEquals(shard.getId(), shard1.getId());
        ctx.put("id", Integer.valueOf(1000));
        shards = simpleRouter.route(SQLMAP4, ctx);
        assertEquals(shards.size(), 1);
        shard = shards.iterator().next();
        assertEquals(shard.getId(), shard1.getId());
        ctx.put("id", Integer.valueOf(10000));
        shards = simpleRouter.route(SQLMAP4, ctx);
        assertEquals(shards.size(), 1);
        shard = shards.iterator().next();
        assertEquals(shard.getId(), shard2.getId());
    }

}
