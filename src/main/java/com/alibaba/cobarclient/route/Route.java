package com.alibaba.cobarclient.route;

import com.alibaba.cobarclient.Shard;
import com.alibaba.cobarclient.expr.Expression;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 */
public class Route {
    private String sqlmap;
    private Expression expression;
    private Set<Shard> shards;

    public Route(String sqlmap, Expression expression, Set<Shard> shards) {
        this.sqlmap = sqlmap;
        this.expression = expression;
        this.shards = shards;
    }

    public boolean apply(String action, Object argument) {
        if (sqlmap == null) return false;
        if (!sqlmap.equals(action)) return false;
        if (expression == null) return true;
        if (expression != null && argument != null && expression.apply(argument)) return true;
        return false;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public Set<Shard> getShards() {
        return shards;
    }

    public void setShards(Set<Shard> shards) {
        this.shards = shards;
    }

    public String getSqlmap() {
        return sqlmap;
    }

    public void setSqlmap(String sqlmap) {
        this.sqlmap = sqlmap;
    }

    @Override
    public String toString() {
        return "Route{" +
                "expression=" + expression +
                ", sqlmap='" + sqlmap + '\'' +
                ", shards=" + shards +
                '}';
    }
}
