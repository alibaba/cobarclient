package com.alibaba.cobarclient;

import com.alibaba.cobarclient.route.Router;
import com.ibatis.sqlmap.client.SqlMapSession;
import com.ibatis.sqlmap.client.event.RowHandler;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.orm.ibatis.SqlMapClientCallback;
import org.springframework.orm.ibatis.SqlMapClientTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 */
public class MysdalSqlmapClientTemplate extends SqlMapClientTemplate implements DisposableBean {
    /**
     * we need target set of the shard to set the boundary of distributed data access and negotiate some default settings for MysdalSqlmapClientTemplate, say, default pool size to use.
     */
    private Set<Shard> shards;
    /**
     * Since Routes should be assigned explicitly, A Router has to be injected explicitly too. Anyway, we are using MysdalSqlmapClientTemplate to do distributed data access, right?
     */
    private Router router;
    private ExecutorService executor;

    /**
     * operation timeout threshold, in milliseconds
     */
    private long timeout = 5 * 1000;
    /**
     * if no external #executor is injected, then we will initialize one to use, and this flag will indicate such alternative so that we can decide whether we need to clean up after.
     */
    private boolean useDefaultExecutor = false;
    /**
     * for operations that only need to be executed against single shard, it's NOT necessary to perform them in common way, that's,
     * submit them to thread pool and execute in parallel.
     * In such scenarios, just reuse SqlMapClientTemplate's easy API and get it done smoothly.
     */
    protected Map<String, SqlMapClientTemplate> CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES = new HashMap<String, SqlMapClientTemplate>();

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        if (shards == null || shards.isEmpty()) throw new IllegalArgumentException("'shards' argument is required.");
        if (router == null) throw new IllegalArgumentException("'router' argument is required");
        if (executor == null) {
            useDefaultExecutor = true;
            executor = new ForkJoinPool(shards.size() * 10);
        }
        for (Shard shard : shards) {
            CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.put(shard.getId(), new SqlMapClientTemplate(shard.getDataSource(), getSqlMapClient()));
        }
    }

    public void destroy() throws Exception {
        if (useDefaultExecutor) {
            executor.shutdown();
        }
    }


    @Override
    public int delete(String statementName) throws DataAccessException {
        return super.delete(statementName);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int delete(String statementName, Object parameterObject) throws DataAccessException {
        return super.delete(statementName, parameterObject);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void delete(String statementName, Object parameterObject, int requiredRowsAffected) throws DataAccessException {
        super.delete(statementName, parameterObject, requiredRowsAffected);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object insert(String statementName) throws DataAccessException {
        return super.insert(statementName);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object insert(String statementName, Object parameterObject) throws DataAccessException {
        return super.insert(statementName, parameterObject);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List queryForList(String statementName) throws DataAccessException {
        return super.queryForList(statementName);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List queryForList(String statementName, Object parameterObject) throws DataAccessException {
        return super.queryForList(statementName, parameterObject);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List queryForList(String statementName, Object parameterObject, int skipResults, int maxResults) throws DataAccessException {
        return super.queryForList(statementName, parameterObject, skipResults, maxResults);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List queryForList(String statementName, int skipResults, int maxResults) throws DataAccessException {
        return super.queryForList(statementName, skipResults, maxResults);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Map queryForMap(String statementName, Object parameterObject, String keyProperty) throws DataAccessException {
        return super.queryForMap(statementName, parameterObject, keyProperty);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Map queryForMap(String statementName, Object parameterObject, String keyProperty, String valueProperty) throws DataAccessException {
        return super.queryForMap(statementName, parameterObject, keyProperty, valueProperty);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object queryForObject(String statementName) throws DataAccessException {
        return super.queryForObject(statementName);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object queryForObject(String statementName, Object parameterObject) throws DataAccessException {
        return super.queryForObject(statementName, parameterObject);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object queryForObject(String statementName, Object parameterObject, Object resultObject) throws DataAccessException {
        return super.queryForObject(statementName, parameterObject, resultObject);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void queryWithRowHandler(String statementName, RowHandler rowHandler) throws DataAccessException {
        super.queryWithRowHandler(statementName, rowHandler);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void queryWithRowHandler(String statementName, Object parameterObject, RowHandler rowHandler) throws DataAccessException {
        super.queryWithRowHandler(statementName, parameterObject, rowHandler);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int update(String statementName, Object parameterObject) throws DataAccessException {
        return super.update(statementName, parameterObject);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void update(String statementName, Object parameterObject, int requiredRowsAffected) throws DataAccessException {
        super.update(statementName, parameterObject, requiredRowsAffected);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int update(String statementName) throws DataAccessException {
        return super.update(statementName);    //To change body of overridden methods use File | Settings | File Templates.
    }


    protected <T> List<T> execute(Set<Shard> shards, final SqlMapClientCallback<T> callback) {
        MultipleCauseException exceptions = new MultipleCauseException();
        Map<String, ResourceBundle> executionContext = new HashMap<String, ResourceBundle>();

        for (Shard shard : shards) {
            try {
                final Connection connection = DataSourceUtils.getConnection(shard.getDataSource());
                Future<T> future = executor.submit(new Callable<T>() {
                    public T call() throws Exception {
                        SqlMapSession session = getSqlMapClient().openSession();
                        try {
                            session.setUserConnection(connection);
                            try {
                                return callback.doInSqlMapClient(session);
                            } catch (SQLException ex) {
                                throw getExceptionTranslator().translate("SqlMapClient operation", null, ex);
                            }
                        } finally {
                            session.close();
                        }
                    }
                });
                executionContext.put(shard.getId(), new ResourceBundle(connection, shard.getDataSource(), future));
            } catch (Exception ex) {
                exceptions.add(ex);
            }
        }

        List<T> resultList = new ArrayList<T>();

        for (Map.Entry<String, ResourceBundle> entry : executionContext.entrySet()) {
            ResourceBundle<T> bundle = entry.getValue();
            try {
                resultList.add(bundle.getFuture().get(timeout, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                exceptions.add(e);
            } catch (ExecutionException e) {
                exceptions.add(e);
            } catch (TimeoutException e) {
                exceptions.add(e);
            } finally {
                try {
                    DataSourceUtils.releaseConnection(bundle.getConnection(), bundle.getDataSource());
                } catch (Exception ex) {
                    logger.debug("Could not close JDBC Connection", ex);
                }
            }
        }

        if (!exceptions.getCauses().isEmpty())
            throw new TransientDataAccessResourceException("one or more errors when performing data access operations against multiple shards", exceptions);

        return resultList;

    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Router getRouter() {
        return router;
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    public Set<Shard> getShards() {
        return shards;
    }

    public void setShards(Set<Shard> shards) {
        this.shards = shards;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}


class ResourceBundle<T> {
    private Future<T> future;
    private Connection connection;
    private DataSource dataSource;

    ResourceBundle(Connection connection, DataSource dataSource, Future<T> future) {
        this.connection = connection;
        this.dataSource = dataSource;
        this.future = future;
    }

    Future<T> getFuture() {
        return future;
    }

    Connection getConnection() {
        return connection;
    }

    DataSource getDataSource() {
        return dataSource;
    }

}