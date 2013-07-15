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
package com.alibaba.cobar.client.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import com.alibaba.cobar.client.datasources.ICobarDataSourceService;

/**
 * use {@link org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy} to wrap all of the data sources we
 * may use in TransactionManager and DAOs. {@link org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy}
 * will only fetch a connection when first statement get executed. So even we
 * start transaction on such data sources which are wrapped by
 * {@link org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy}, there is no performance penalty at
 * not.
 *
 *
 *
 * @author fujohnwang
 * @since 1.0, Jan 28, 2010
 */
public class MultipleDataSourcesTransactionManager extends
AbstractPlatformTransactionManager implements InitializingBean {
	protected transient Logger logger = org.slf4j.LoggerFactory
			.getLogger(MultipleDataSourcesTransactionManager.class);

	private static final long serialVersionUID = 4712923770419532385L;

	private ICobarDataSourceService cobarDataSourceService;
	private List<PlatformTransactionManager> transactionManagers = new ArrayList<PlatformTransactionManager>();

	@Override
	protected Object doGetTransaction() throws TransactionException {
		return new ArrayList<DefaultTransactionStatus>();
	}

	/**
	 * We need to disable transaction synchronization so that the shared
	 * transaction synchronization state will not collide with each other. BUT,
	 * for LOB creators to use, we have to pay attention here:
	 * <ul>
	 * <li>if the LOB creator use standard preparedStatement methods, this
	 * transaction synchronization setting is OK;</li>
	 * <li>if the LOB creator don't use standard PS methods, you have to find
	 * other way to make sure the resources your LOB creator used should be
	 * cleaned up after the transaction.</li>
	 * </ul>
	 */
	@Override
	protected void doBegin(Object transactionObject,
			TransactionDefinition transactionDefinition)
					throws TransactionException {
		@SuppressWarnings("unchecked")
		List<DefaultTransactionStatus> list = (List<DefaultTransactionStatus>) transactionObject;
		for (PlatformTransactionManager transactionManager : transactionManagers) {
			DefaultTransactionStatus element = (DefaultTransactionStatus) transactionManager
					.getTransaction(transactionDefinition);
			list.add(element);
		}
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		@SuppressWarnings("unchecked")
		List<DefaultTransactionStatus> list = 
		      (List<DefaultTransactionStatus>) status.getTransaction();

		logger.info("prepare to commit transactions on multiple data sources.");
		Validate.isTrue(list.size() <= this.getTransactionManagers().size());

		TransactionException lastException = null;
		for(int i=list.size()-1; i>=0;i--){
			PlatformTransactionManager transactionManager=this.getTransactionManagers().get(i);
			TransactionStatus localTransactionStatus=list.get(i);
			
			try{
				transactionManager.commit(localTransactionStatus);
			}
			catch (TransactionException e) {
				lastException=e;
				logger.error("Error in commit", e);

			}
		}
		if (lastException != null) {
			throw lastException;
			// Rollback will ensue as long as rollbackOnCommitFailure=true
		}

	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		@SuppressWarnings("unchecked")
		List<DefaultTransactionStatus> list = 
		     (List<DefaultTransactionStatus>) status.getTransaction();

		logger.info("prepare to rollback transactions on multiple data sources.");
		Validate.isTrue(list.size() <= this.getTransactionManagers().size());

		TransactionException lastException = null;
		for(int i=list.size()-1; i>=0; i--){
			PlatformTransactionManager transactionManager=this.getTransactionManagers().get(i);
			TransactionStatus localTransactionStatus=list.get(i);
			
			try {
				transactionManager.rollback(localTransactionStatus);
			} catch (TransactionException e) {
				// Log exception and try to complete rollback
				lastException = e;
				logger.error("error occured when rolling back the transaction. \n{}",e);
			}
		}
		
		if (lastException != null) {
			throw lastException;
		}
	}

	public void setCobarDataSourceService(
			ICobarDataSourceService cobarDataSourceService) {
		this.cobarDataSourceService = cobarDataSourceService;
	}

	public ICobarDataSourceService getCobarDataSourceService() {
		return cobarDataSourceService;
	}

	public void afterPropertiesSet() throws Exception {
		Validate.notNull(cobarDataSourceService);
		for (DataSource dataSource : getCobarDataSourceService()
				.getDataSources().values()) {
			PlatformTransactionManager txManager = this.createTransactionManager(dataSource);
			getTransactionManagers().add(txManager);
		}
		//Collections.reverse(getTransactionManagers());
	}


	protected PlatformTransactionManager createTransactionManager(DataSource dataSource){
		return new DataSourceTransactionManager(dataSource);
	}


	public List<PlatformTransactionManager> getTransactionManagers() {
		return transactionManagers;
	}

}
