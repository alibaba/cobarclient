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
 package com.alibaba.cobar.client.support.execution;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.orm.ibatis.SqlMapClientCallback;

import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapSession;

public class DefaultConcurrentRequestProcessor implements IConcurrentRequestProcessor {

    private transient final Logger logger = LoggerFactory
                                                  .getLogger(DefaultConcurrentRequestProcessor.class);

    private SqlMapClient           sqlMapClient;

    public DefaultConcurrentRequestProcessor() {
    }

    public DefaultConcurrentRequestProcessor(SqlMapClient sqlMapClient) {
        this.sqlMapClient = sqlMapClient;
    }

    public List<Object> process(List<ConcurrentRequest> requests) {
        List<Object> resultList = new ArrayList<Object>();

        if (CollectionUtils.isEmpty(requests))
            return resultList;

        List<RequestDepository> requestsDepo = fetchConnectionsAndDepositForLaterUse(requests);
        final CountDownLatch latch = new CountDownLatch(requestsDepo.size());
        List<Future<Object>> futures = new ArrayList<Future<Object>>();
        try {

            for (RequestDepository rdepo : requestsDepo) {
                ConcurrentRequest request = rdepo.getOriginalRequest();
                final SqlMapClientCallback action = request.getAction();
                final Connection connection = rdepo.getConnectionToUse();

                futures.add(request.getExecutor().submit(new Callable<Object>() {
                    public Object call() throws Exception {
                        try {
                            return executeWith(connection, action);
                        } finally {
                            latch.countDown();
                        }
                    }
                }));
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            }

        } finally {
            for (RequestDepository depo : requestsDepo) {
                Connection springCon = depo.getConnectionToUse();
                DataSource dataSource = depo.getOriginalRequest().getDataSource();
                try {
                    if (springCon != null) {
                        if (depo.isTransactionAware()) {
                            springCon.close();
                        } else {
                            DataSourceUtils.doReleaseConnection(springCon, dataSource);
                        }
                    }
                } catch (Throwable ex) {
                    logger.info("Could not close JDBC Connection", ex);
                }
            }
        }

        fillResultListWithFutureResults(futures, resultList);

        return resultList;
    }

    protected Object executeWith(Connection connection, SqlMapClientCallback action) {
        SqlMapSession session = getSqlMapClient().openSession();
        try {
            try {
                session.setUserConnection(connection);
            } catch (SQLException e) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", e);
            }
            try {
                return action.doInSqlMapClient(session);
            } catch (SQLException ex) {
                throw new SQLErrorCodeSQLExceptionTranslator().translate("SqlMapClient operation",
                        null, ex);
            }
        } finally {
            session.close();
        }
    }

    private void fillResultListWithFutureResults(List<Future<Object>> futures,
                                                 List<Object> resultList) {
        for (Future<Object> future : futures) {
            try {
                resultList.add(future.get());
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            } catch (ExecutionException e) {
                throw new ConcurrencyFailureException("something goes wrong in processing", e);
            }
        }
    }

    private List<RequestDepository> fetchConnectionsAndDepositForLaterUse(
                                                                          List<ConcurrentRequest> requests) {
        List<RequestDepository> depos = new ArrayList<RequestDepository>();
        for (ConcurrentRequest request : requests) {
            DataSource dataSource = request.getDataSource();

            Connection springCon = null;
            boolean transactionAware = (dataSource instanceof TransactionAwareDataSourceProxy);
            try {
                springCon = (transactionAware ? dataSource.getConnection() : DataSourceUtils
                        .doGetConnection(dataSource));
            } catch (SQLException ex) {
                throw new CannotGetJdbcConnectionException("Could not get JDBC Connection", ex);
            }

            RequestDepository depo = new RequestDepository();
            depo.setOriginalRequest(request);
            depo.setConnectionToUse(springCon);
            depo.setTransactionAware(transactionAware);
            depos.add(depo);
        }

        return depos;
    }

    public void setSqlMapClient(SqlMapClient sqlMapClient) {
        Validate.notNull(sqlMapClient);
        this.sqlMapClient = sqlMapClient;
    }

    public SqlMapClient getSqlMapClient() {
        return sqlMapClient;
    }

}
