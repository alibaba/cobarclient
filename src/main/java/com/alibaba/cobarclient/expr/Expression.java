package com.alibaba.cobarclient.expr;

public interface Expression {
    String expr();

    boolean apply(Object context);
}



