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
 package com.alibaba.cobar.client.datasources.ha;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.HotSwappableTargetSource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;

public class PassiveEventHotSwappableAdvice implements MethodInterceptor, InitializingBean {
    private final transient Logger         logger              = LoggerFactory
                                                                       .getLogger(PassiveEventHotSwappableAdvice.class);

    private static final Integer           DEFAULT_RETRY_TIMES = 3;

    private SQLStateSQLExceptionTranslator sqlExTranslator     = new SQLStateSQLExceptionTranslator();

    /**
     * threshold to indicate until how many times we will stop hot swap between
     * HA data sources.<br>
     * default behavior is always swap(with threshold value to be
     * Integer.MAX_VALUE).
     */
    private Integer                        swapTimesThreshold  = Integer.MAX_VALUE;

    /**
     * In fact, this is not necessary since DataSource implementations like C3P0
     * or DBCP has properties to enable connection retry or recovery. as long as
     * you configure the underlying data source implementation instances, they
     * will do this job for you.
     */
    private Integer                        retryTimes          = DEFAULT_RETRY_TIMES;
    /**
     * time unit in milliseconds
     */
    private long                           retryInterval       = 1000;

    private String                         detectingSql        = "SELECT 1";

    private HotSwappableTargetSource       targetSource;
    private DataSource                     mainDataSource;
    private DataSource                     standbyDataSource;

    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (!StringUtils.equalsIgnoreCase(invocation.getMethod().getName(), "getConnection")) {
            return invocation.proceed();
        }

        try {
            return invocation.proceed();
            // need to check with detecting sql?

        } catch (Throwable t) {
            if (t instanceof SQLException) {
                // we use SQLStateSQLExceptionTranslator to translate SQLExceptions , but it doesn't mean it will work as we expected, 
                // so maybe more scope should be covered. we will check out later with runtime data statistics.
                DataAccessException dae = sqlExTranslator.translate(
                        "translate to check whether it's a resource failure exception", null,
                        (SQLException) t);
                if (dae instanceof DataAccessResourceFailureException) {
                    logger.warn("failed to get Connection from data source with exception:\n{}", t);
                    doSwap();
                    return invocation.getMethod().invoke(targetSource.getTarget(),
                            invocation.getArguments());
                }
            }
            // other exception conditions should be handled by application, 
            // 'cause we don't have enough context information to decide what to do here.
            throw t;
        }
    }

    private void doSwap() {
        synchronized (targetSource) {
            DataSource target = (DataSource) getTargetSource().getTarget();
            if (target == mainDataSource) {
                logger.warn("hot swap from '" + target + "' to '" + standbyDataSource + "'.");
                getTargetSource().swap(standbyDataSource);
            } else {
                logger.warn("hot swap from '" + target + "' to '" + mainDataSource + "'.");
                getTargetSource().swap(mainDataSource);
            }
        }
    }

    public Integer getSwapTimesThreshold() {
        return swapTimesThreshold;
    }

    public void setSwapTimesThreshold(Integer swapTimesThreshold) {
        this.swapTimesThreshold = swapTimesThreshold;
    }

    public HotSwappableTargetSource getTargetSource() {
        return targetSource;
    }

    public void setTargetSource(HotSwappableTargetSource targetSource) {
        this.targetSource = targetSource;
    }

    public void afterPropertiesSet() throws Exception {
        if (targetSource == null || mainDataSource == null || standbyDataSource == null) {
            throw new IllegalArgumentException(
                    "the target source, main data source and standby data source must be set.");
        }
    }

    public void setRetryTimes(Integer retryTimes) {
        this.retryTimes = retryTimes;
    }

    public Integer getRetryTimes() {
        return retryTimes;
    }

    public DataSource getMainDataSource() {
        return mainDataSource;
    }

    public void setMainDataSource(DataSource mainDataSource) {
        this.mainDataSource = mainDataSource;
    }

    public DataSource getStandbyDataSource() {
        return standbyDataSource;
    }

    public void setStandbyDataSource(DataSource standbyDataSource) {
        this.standbyDataSource = standbyDataSource;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setDetectingSql(String detectingSql) {
        this.detectingSql = detectingSql;
    }

    public String getDetectingSql() {
        return detectingSql;
    }

}
