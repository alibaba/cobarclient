package com.alibaba.cobarclient.route;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class RouteTest {

    public static String SQLMAP_NAME_FIXTURE = "com.alibaba.domain.Offer.insert";

    @Test
    public void testSqlmapAndExpressionRoute() {

        assertFalse(new Route(null, null, null).apply(SQLMAP_NAME_FIXTURE, new Object()));
        assertTrue(new Route(SQLMAP_NAME_FIXTURE, null, null).apply(SQLMAP_NAME_FIXTURE, null));


    }

    @Test
    public void testSqlmapRoute() {

    }
}
