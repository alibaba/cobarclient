package com.alibaba.cobarclient;

import javax.sql.DataSource;

/**
 * A Shard will be the descriptor for target database shard and referred through cobarclient2.0 API.
 */
public class Shard {
    private String id;
    private DataSource dataSource;
    private String description;

    public Shard(String id, DataSource dataSource, String description) {
        this.id = id;
        this.dataSource = dataSource;
        this.description = description;
    }

    public Shard(String id, DataSource dataSource) {
        this(id, dataSource, null);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Shard{" +
                "dataSource=" + dataSource +
                ", id='" + id + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
