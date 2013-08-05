package com.alibaba.cobarclient.expr;


public abstract class AbstractExpression implements Expression {
    protected String expression;

    public AbstractExpression(String expression) {
        this.expression = expression;
    }

    public String expr() {
        return this.expression;
    }
}