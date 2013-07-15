package com.alibaba.cobar.client.test.services;

import org.springframework.orm.ibatis.SqlMapClientTemplate;

public abstract class AbstractOfferService implements IOfferService {
    private SqlMapClientTemplate sqlMapClientTemplate;

    public void setSqlMapClientTemplate(SqlMapClientTemplate sqlMapClientTemplate) {
        this.sqlMapClientTemplate = sqlMapClientTemplate;
    }

    public SqlMapClientTemplate getSqlMapClientTemplate() {
        return sqlMapClientTemplate;
    }

}
