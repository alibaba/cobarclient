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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.HotSwappableTargetSource;

/**
 * The standard to switch in this job is vague, because usually we will figure
 * out different SQLException type with error code to decide whether current
 * SQLException indicates a connection failure or something else. but as per
 * Hexianmao's statement, it's not necessary to be so, because sometimes, DBA or
 * someone will cause SQLException with some operations on purpose so that the
 * data source can be switched out to be maintained.<br>
 * So we adapt the failover checking logic from current Cobar's HAPool with some
 * code structure adjustment.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class FailoverMonitorJob implements Runnable {

    private transient final Logger   logger   = LoggerFactory.getLogger(FailoverMonitorJob.class);

    private String                   detectingSQL;
    /**
     * time unit in milliseconds
     */
    private long                     detectingRequestTimeout;

    private long                     recheckInterval;
    private int                      recheckTimes;

    private HotSwappableTargetSource hotSwapTargetSource;
    private DataSource               masterDataSource;
    private DataSource               standbyDataSource;
    private DataSource               masterDetectorDataSource;
    private DataSource               standbyDetectorDataSource;

    /**
     * first time it should be referenced to masterDetectorDataSource.
     */
    private DataSource               currentDetectorDataSource;

    /**
     * Since {@link FailoverMonitorJob} will be scheduled to run in sequence,
     * One executor as instance field is ok.<br>
     * This executor will be used to execute detecting logic asynchronously, if
     * the execution exceeds given timeout threshold, we will check again before
     * switching to standby data source.
     */
    private ExecutorService          executor;
    
    public FailoverMonitorJob(ExecutorService es)
    {
        Validate.notNull(es);
        this.executor = es;
    }

    public void run() {
        Future<Integer> future = executor.submit(new Callable<Integer>() {

            public Integer call() throws Exception {
                Integer result = -1;

                for (int i = 0; i < getRecheckTimes(); i++) {
                    Connection conn = null;
                    try {
                        conn = getCurrentDetectorDataSource().getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(getDetectingSQL());
                        pstmt.execute();
                        if (pstmt != null) {
                            pstmt.close();
                        }
                        result = 0;
                        break;
                    } catch (Exception e) {
                        logger.warn("(" + (i + 1) + ") check with failure. sleep ("
                                + getRecheckInterval() + ") for next round check.");
                        try {
                            TimeUnit.MILLISECONDS.sleep(getRecheckInterval());
                        } catch (InterruptedException e1) {
                            logger.warn("interrupted when waiting for next round rechecking.");
                        }
                        continue;
                    } finally {
                        if (conn != null) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                logger.warn("failed to close checking connection:\n", e);
                            }
                        }
                    }
                }
                return result;
            }
        });

        try {
            Integer result = future.get(getDetectingRequestTimeout(), TimeUnit.MILLISECONDS);
            if (result == -1) {
                doSwap();
            }
        } catch (InterruptedException e) {
            logger.warn("interrupted when getting query result in FailoverMonitorJob.");
        } catch (ExecutionException e) {
            logger.warn("exception occured when checking failover status in FailoverMonitorJob");
        } catch (TimeoutException e) {
            logger.warn("exceed DetectingRequestTimeout threshold. Switch to standby data source.");
            doSwap();
        }
    }

    private  void doSwap() {
        synchronized(hotSwapTargetSource){
            DataSource target = (DataSource) getHotSwapTargetSource().getTarget();
            if (target == masterDataSource) {
                getHotSwapTargetSource().swap(standbyDataSource);
                currentDetectorDataSource = standbyDetectorDataSource;
            } else {
                getHotSwapTargetSource().swap(masterDataSource);
                currentDetectorDataSource = masterDetectorDataSource;
            }
        }
    }

    public String getDetectingSQL() {
        return detectingSQL;
    }

    public void setDetectingSQL(String detectingSQL) {
        this.detectingSQL = detectingSQL;
    }

    public long getDetectingRequestTimeout() {
        return detectingRequestTimeout;
    }

    public void setDetectingRequestTimeout(long detectingRequestTimeout) {
        this.detectingRequestTimeout = detectingRequestTimeout;
    }

    public HotSwappableTargetSource getHotSwapTargetSource() {
        return hotSwapTargetSource;
    }

    public void setHotSwapTargetSource(HotSwappableTargetSource hotSwapTargetSource) {
        this.hotSwapTargetSource = hotSwapTargetSource;
    }

    public DataSource getMasterDataSource() {
        return masterDataSource;
    }

    public void setMasterDataSource(DataSource masterDataSource) {
        this.masterDataSource = masterDataSource;
    }

    public DataSource getStandbyDataSource() {
        return standbyDataSource;
    }

    public void setStandbyDataSource(DataSource standbyDataSource) {
        this.standbyDataSource = standbyDataSource;
    }

    public void setRecheckInterval(long recheckInterval) {
        this.recheckInterval = recheckInterval;
    }

    public long getRecheckInterval() {
        return recheckInterval;
    }

    public void setRecheckTimes(int recheckTimes) {
        this.recheckTimes = recheckTimes;
    }

    public int getRecheckTimes() {
        return recheckTimes;
    }

    public void setMasterDetectorDataSource(DataSource masterDetectorDataSource) {
        this.masterDetectorDataSource = masterDetectorDataSource;
    }

    public DataSource getMasterDetectorDataSource() {
        return masterDetectorDataSource;
    }

    public void setStandbyDetectorDataSource(DataSource standbyDetectorDataSource) {
        this.standbyDetectorDataSource = standbyDetectorDataSource;
    }

    public DataSource getStandbyDetectorDataSource() {
        return standbyDetectorDataSource;
    }

    public void setCurrentDetectorDataSource(DataSource currentDetectorDataSource) {
        this.currentDetectorDataSource = currentDetectorDataSource;
    }

    public DataSource getCurrentDetectorDataSource() {
        return currentDetectorDataSource;
    }

}
