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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.HotSwappableTargetSource;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.cobar.client.datasources.CobarDataSourceDescriptor;
import com.alibaba.cobar.client.datasources.DefaultCobarDataSourceService;

/**
 * FailoverHotSwapDataSourceCreator will create a Data Source Proxy that will
 * handle Failover on 2 DataSouces which are in same HA group.<br>
 * The {@link FailoverHotSwapDataSourceCreator} will enable 2 type of failover
 * strategies:
 * <ol>
 * <li>Passive Monitoring Failover Strategy : intercept method invocation status
 * on DataSource to decide whether to perform failover action.</li>
 * <li>Active Monitoring Failover Strategy : send detecting SQL to target data
 * source in a fixed time period to check the status of target data source. If
 * the monitoring action failed, the failover action will be taken.</li>
 * </ol> {@link FailoverHotSwapDataSourceCreator#passiveFailoverEnable} and
 * {@link FailoverHotSwapDataSourceCreator#positiveFailoverEnable} are
 * indicators that will be used to control which failover strategy will be used
 * or both.<br>
 * if positive failover strategy is enabled, there are 2 things need paying
 * attention to. firstly, a {@link #detectingSql} must be provided which will be
 * used to detect data source status. secondly, the value of
 * {@link #detectingTimeoutThreshold} should be less (even equals) than
 * {@link #monitorPeriod}.
 * 
 * @author fujohnwang
 * @see DefaultCobarDataSourceService
 */
public class FailoverHotSwapDataSourceCreator implements IHADataSourceCreator, InitializingBean,
        DisposableBean {

    private transient final Logger                                      logger                    = LoggerFactory
                                                                                                          .getLogger(FailoverHotSwapDataSourceCreator.class);
    /**
     * indicator that's used to indicate whether to enable passive failover
     * support.
     */
    private boolean                                                     passiveFailoverEnable     = false;
    /**
     * indicator that's used to indicate whether to enable positive failover
     * support.
     */
    private boolean                                                     positiveFailoverEnable    = true;
    /**
     * register scheduling job in synchronization to check DB status.
     */
    private ConcurrentMap<ScheduledFuture<?>, ScheduledExecutorService> schedulerFutures          = new ConcurrentHashMap<ScheduledFuture<?>, ScheduledExecutorService>();
    /**
     * hold executor reference for later disposal.
     */
    private List<ExecutorService>                                       jobExecutorRegistry       = new ArrayList<ExecutorService>();
    /**
     * time unit in milliseconds
     */
    private long                                                        monitorPeriod             = 15 * 1000;
    /**
     * initial time delay before starting the positive HA monitoring job
     */
    private int                                                         initialDelay              = 0;
    /**
     * the detecting SQL that will be used to detect data source status.
     */
    private String                                                      detectingSql;
    /**
     * detecting timeout threshold with time unit in milliseconds.<br>
     * the value of this usually should be less than {@link #monitorPeriod}.
     */
    private long                                                        detectingTimeoutThreshold = 15 * 1000;
    /**
     * time unit in milliseconds
     */
    private long                                                        recheckInterval           = 5 * 1000;

    private int                                                         recheckTimes              = 3;

    public DataSource createHADataSource(CobarDataSourceDescriptor descriptor) throws Exception {
        DataSource activeDataSource = descriptor.getTargetDataSource();
        DataSource standbyDataSource = descriptor.getStandbyDataSource();
        if (activeDataSource == null && standbyDataSource == null) {
            throw new IllegalArgumentException("must have at least one data source active.");
        }
        if (activeDataSource == null || standbyDataSource == null) {
            logger.warn("only one data source is available for use, so no HA support.");
            if (activeDataSource == null) {
                return standbyDataSource;
            }
            return activeDataSource;
        }

        HotSwappableTargetSource targetSource = new HotSwappableTargetSource(activeDataSource);
        ProxyFactory pf = new ProxyFactory();
        pf.setInterfaces(new Class[] { DataSource.class });
        pf.setTargetSource(targetSource);
        
        
        if (isPositiveFailoverEnable()) {
            DataSource targetDetectorDataSource = descriptor.getTargetDetectorDataSource();
            DataSource standbyDetectorDataSource = descriptor.getStandbyDetectorDataSource();
            if (targetDetectorDataSource == null || standbyDetectorDataSource == null) {
                throw new IllegalArgumentException(
                        "targetDetectorDataSource or standbyDetectorDataSource can't be null if positive failover is enabled.");
            }
            // 1. create active monitoring job for failover event
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            ExecutorService jobExecutor = Executors.newFixedThreadPool(1);
            jobExecutorRegistry.add(jobExecutor);
            FailoverMonitorJob job = new FailoverMonitorJob(jobExecutor);
            //    1.1  inject dependencies
            job.setHotSwapTargetSource(targetSource);
            job.setMasterDataSource(activeDataSource);
            job.setStandbyDataSource(standbyDataSource);
            job.setMasterDetectorDataSource(targetDetectorDataSource);
            job.setStandbyDetectorDataSource(standbyDetectorDataSource);
            job.setCurrentDetectorDataSource(targetDetectorDataSource);
            job.setDetectingRequestTimeout(getDetectingTimeoutThreshold());
            job.setDetectingSQL(getDetectingSql());
            job.setRecheckInterval(recheckInterval);
            job.setRecheckTimes(recheckTimes);
            //    1.2  start scheduling and keep reference for canceling and shutdown
            ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(job, initialDelay,
                    monitorPeriod, TimeUnit.MILLISECONDS);
            schedulerFutures.put(future, scheduler);
        }

        if (isPassiveFailoverEnable()) {
            // 2. create data source proxy with passive event advice
            PassiveEventHotSwappableAdvice advice = new PassiveEventHotSwappableAdvice();
            advice.setRetryInterval(recheckInterval);
            advice.setRetryTimes(recheckTimes);
            advice.setDetectingSql(detectingSql);
            advice.setTargetSource(targetSource);
            advice.setMainDataSource(activeDataSource);
            advice.setStandbyDataSource(standbyDataSource);
            pf.addAdvice(advice);
        }

        return (DataSource) pf.getProxy();
    }

    public void afterPropertiesSet() throws Exception {
        if (!isPassiveFailoverEnable() && !isPositiveFailoverEnable()) {
            return;
        }
        if (StringUtils.isEmpty(detectingSql)) {
            throw new IllegalArgumentException(
                    "A 'detectingSql' should be provided if positive failover function is enabled.");
        }

        if (monitorPeriod <= 0 || detectingTimeoutThreshold <= 0 || recheckInterval <= 0
                || recheckTimes <= 0) {
            throw new IllegalArgumentException(
                    "'monitorPeriod' OR 'detectingTimeoutThreshold' OR 'recheckInterval' OR 'recheckTimes' must be positive.");
        }

        if (isPositiveFailoverEnable()) {
            if ((detectingTimeoutThreshold > monitorPeriod)) {
                throw new IllegalArgumentException(
                        "the 'detectingTimeoutThreshold' should be less(or equals) than 'monitorPeriod'.");
            }

            if ((recheckInterval * recheckTimes) > detectingTimeoutThreshold) {
                throw new IllegalArgumentException(
                        " 'recheckInterval * recheckTimes' can not be longer than 'detectingTimeoutThreshold'");
            }
        }

    }

    public void destroy() throws Exception {

        for (Map.Entry<ScheduledFuture<?>, ScheduledExecutorService> e : schedulerFutures
                .entrySet()) {
            ScheduledFuture<?> future = e.getKey();
            ScheduledExecutorService scheduler = e.getValue();
            future.cancel(true);
            shutdownExecutor(scheduler);
        }

        for (ExecutorService executor : jobExecutorRegistry) {
            shutdownExecutor(executor);
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.warn("interrupted when shutting down executor service.");
        }
    }

    /**
     * set the time period of positive database status detection, monitor will
     * send detecting request in a interval of such time period.<br>
     * 
     * @param monitorPeriod
     */
    public void setMonitorPeriod(long monitorPeriod) {
        this.monitorPeriod = monitorPeriod;
    }

    public long getMonitorPeriod() {
        return monitorPeriod;
    }

    /**
     * set the initial time delay before launching the monitoring job. default
     * value is 0, that's, start at once.
     * 
     * @param initialDelay
     */
    public void setInitialDelay(int initialDelay) {
        this.initialDelay = initialDelay;
    }

    public int getInitialDelay() {
        return initialDelay;
    }

    /**
     * set true to enable passive fail over support.<br>
     * default is false.
     * 
     * @param passiveFailoverEnable
     */
    public void setPassiveFailoverEnable(boolean passiveFailoverEnable) {
        this.passiveFailoverEnable = passiveFailoverEnable;
    }

    public boolean isPassiveFailoverEnable() {
        return passiveFailoverEnable;
    }

    /**
     * set false to disable positive fail over support, default is true too.
     * 
     * @param positiveFailoverEnable
     */
    public void setPositiveFailoverEnable(boolean positiveFailoverEnable) {
        this.positiveFailoverEnable = positiveFailoverEnable;
    }

    public boolean isPositiveFailoverEnable() {
        return positiveFailoverEnable;
    }

    /**
     * set the detecting sql that will be used to detect whether the status of
     * target database is OK.<br>
     * usually, it's better to assign an update SQL instead of a select one.<br>
     * 
     * @param detectingSql
     */
    public void setDetectingSql(String detectingSql) {
        this.detectingSql = detectingSql;
    }

    public String getDetectingSql() {
        return detectingSql;
    }

    /**
     * set the timeout that the detecting request doesn't return in such a time
     * period, it's should be less than {@link #monitorPeriod}.
     * 
     * @param detectingTimeoutThreshold
     */
    public void setDetectingTimeoutThreshold(long detectingTimeoutThreshold) {
        this.detectingTimeoutThreshold = detectingTimeoutThreshold;
    }

    public long getDetectingTimeoutThreshold() {
        return detectingTimeoutThreshold;
    }

    /**
     * when a detecting request fails, to make sure it's not a problem
     * occasionally, we will send another or more detecting request to detect
     * the status of database, the recheckInterval is the time interval that we
     * use to decide in which period that we should send next detecting request.
     * 
     * @param recheckInterval
     */
    public void setRecheckInterval(long recheckInterval) {
        this.recheckInterval = recheckInterval;
    }

    public long getRecheckInterval() {
        return recheckInterval;
    }

    /**
     * if a detecting request fails, we will send another or more to make sure,
     * this property will tell how many more requests should be send.
     * 
     * @param recheckTimes
     */
    public void setRecheckTimes(int recheckTimes) {
        this.recheckTimes = recheckTimes;
    }

    public int getRecheckTimes() {
        return recheckTimes;
    }

}
