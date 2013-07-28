package com.alibaba.cobarclient.expr;

import org.mvel2.MVEL;
import org.mvel2.integration.impl.MapVariableResolverFactory;

import java.util.HashMap;
import java.util.Map;

public class MVELExpression extends AbstractExpression {

    private Map<String, Object> functions;

    public MVELExpression(String expression) {
        super(expression);
    }

    public MVELExpression(String expression, Map<String, Object> functions) {
        super(expression);
        this.functions = new HashMap<String, Object>();
        this.functions.putAll(functions);
    }

    public boolean apply(Object context) {
        Map<String, Object> vrs = new HashMap<String, Object>();
        vrs.putAll(this.functions);
        vrs.put("$ROOT", context);
        return MVEL.evalToBoolean(expression, context, new MapVariableResolverFactory(vrs));
    }
}
