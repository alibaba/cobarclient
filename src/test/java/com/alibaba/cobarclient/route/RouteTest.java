package com.alibaba.cobarclient.route;

import com.alibaba.cobarclient.expr.MVELExpression;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class RouteTest {

    public static String SQLMAP_NAME_FIXTURE = "com.alibaba.domain.Offer.insert";
    public static String EXPRESSION_FIXTURE = "sqlmap==\"insert\"";

    @Test
    public void testRoute() {
        assertFalse(new Route(null, null, null).apply(SQLMAP_NAME_FIXTURE, new Object()));
        assertTrue(new Route(SQLMAP_NAME_FIXTURE, null, null).apply(SQLMAP_NAME_FIXTURE, null));
        assertTrue(new Route(SQLMAP_NAME_FIXTURE, null, null).apply(SQLMAP_NAME_FIXTURE, new Object()));
        assertFalse(new Route(SQLMAP_NAME_FIXTURE, new MVELExpression(""), null).apply(SQLMAP_NAME_FIXTURE, null));

        Route routeAsContext = new Route("insert", null, null);
        assertTrue(new Route(SQLMAP_NAME_FIXTURE, new MVELExpression(EXPRESSION_FIXTURE), null).apply(SQLMAP_NAME_FIXTURE, routeAsContext));
        assertFalse(new Route("", new MVELExpression(EXPRESSION_FIXTURE), null).apply(SQLMAP_NAME_FIXTURE, routeAsContext));
    }

}
