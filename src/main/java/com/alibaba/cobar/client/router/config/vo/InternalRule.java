/**
 * Copyright 1999-2011 Alibaba Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.alibaba.cobar.client.router.config.vo;

public class InternalRule {

    private String namespace;
    private String sqlmap;
    private String shardingExpression;
    private String shards;
    /**
     * this field is not used for now, because it's still in leverage whether
     * it's proper to bind merging information into a routing concern.
     */
    private String merger;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getSqlmap() {
        return sqlmap;
    }

    public void setSqlmap(String sqlmap) {
        this.sqlmap = sqlmap;
    }

    public String getShardingExpression() {
        return shardingExpression;
    }

    public void setShardingExpression(String shardingExpression) {
        this.shardingExpression = shardingExpression;
    }

    public String getShards() {
        return shards;
    }

    public void setShards(String shards) {
        this.shards = shards;
    }

    /**
     * set the bean name of merger to use.
     * 
     * @param merger, the bean name in the container.
     */
    public void setMerger(String merger) {
        this.merger = merger;
    }

    /**
     * @return the bean name of the merger.
     */
    public String getMerger() {
        return merger;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
        result = prime * result
                + ((shardingExpression == null) ? 0 : shardingExpression.hashCode());
        result = prime * result + ((shards == null) ? 0 : shards.hashCode());
        result = prime * result + ((sqlmap == null) ? 0 : sqlmap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InternalRule other = (InternalRule) obj;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        } else if (!namespace.equals(other.namespace))
            return false;
        if (shardingExpression == null) {
            if (other.shardingExpression != null)
                return false;
        } else if (!shardingExpression.equals(other.shardingExpression))
            return false;
        if (shards == null) {
            if (other.shards != null)
                return false;
        } else if (!shards.equals(other.shards))
            return false;
        if (sqlmap == null) {
            if (other.sqlmap != null)
                return false;
        } else if (!sqlmap.equals(other.sqlmap))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "InternalRule [namespace=" + namespace + ", shardingExpression="
                + shardingExpression + ", shards=" + shards + ", sqlmap=" + sqlmap + "]";
    }
}
