package com.alibaba.cobarclient;

import com.alibaba.cobarclient.route.Router;
import com.ibatis.sqlmap.client.SqlMapExecutor;
import com.ibatis.sqlmap.client.SqlMapSession;
import com.ibatis.sqlmap.client.event.RowHandler;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
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
        return this.delete(statementName, null);
    }

    @Override
    public int delete(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.delete(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).delete(statementName, parameterObject);
        } else {
            List results = execute(shards, new SqlMapClientCallback<Integer>() {
                public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.delete(statementName, parameterObject);
                }
            });
            int count = 0;
            for (Object i : results) {
                count += (Integer) i;
            }
            return count;
        }
    }

    @Override
    public void delete(String statementName, Object parameterObject, int requiredRowsAffected) throws DataAccessException {
        int actualRowsAffected = this.delete(statementName, parameterObject);
        if (actualRowsAffected != requiredRowsAffected) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                    statementName, requiredRowsAffected, actualRowsAffected);
        }
    }

    @Override
    public Object insert(String statementName) throws DataAccessException {
        return this.insert(statementName, null);
    }

    @Override
    public Object insert(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.insert(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).insert(statementName, parameterObject);
        } else {
            return execute(shards, new SqlMapClientCallback<Object>() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.insert(statementName, parameterObject);
                }
            });
        }
    }

    @Override
    public List queryForList(String statementName) throws DataAccessException {
        return this.queryForList(statementName, null);
    }

    @Override
    public List queryForList(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForList(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForList(statementName, parameterObject);
        } else {
            return queryForListBase(shards, new SqlMapClientCallback<List>() {
                public List doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForList(statementName, parameterObject);
                }
            });
        }
    }

    @Override
    public List queryForList(final String statementName, final Object parameterObject, final int skipResults, final int maxResults) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForList(statementName, parameterObject, skipResults, maxResults);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForList(statementName, parameterObject, skipResults, maxResults);
        } else {
            return queryForListBase(shards, new SqlMapClientCallback<List>() {
                public List doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForList(statementName, parameterObject, skipResults, maxResults);
                }
            });
        }

    }

    @Override
    public List queryForList(String statementName, int skipResults, int maxResults) throws DataAccessException {
        return this.queryForList(statementName, null, skipResults, maxResults);
    }

    /**
     * collection and flatten the result list
     */
    protected List queryForListBase(Set<Shard> shards, SqlMapClientCallback<List> callback) {
        List<List> results = execute(shards, callback);
        List resultList = new ArrayList();         // FLATTEN the list, miss FP pattern here.
        for (List lst : results) {
            resultList.addAll(lst);
        }
        return resultList;
    }

    @Override
    public Map queryForMap(final String statementName, final Object parameterObject, final String keyProperty) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForMap(statementName, parameterObject, keyProperty);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForMap(statementName, parameterObject, keyProperty);
        } else {
            return queryForMapBase(shards, new SqlMapClientCallback<Map>() {
                public Map doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForMap(statementName, parameterObject, keyProperty);
                }
            });
        }
    }

    @Override
    public Map queryForMap(final String statementName, final Object parameterObject, final String keyProperty, final String valueProperty) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForMap(statementName, parameterObject, keyProperty, valueProperty);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForMap(statementName, parameterObject, keyProperty, valueProperty);
        } else {
            return queryForMapBase(shards, new SqlMapClientCallback<Map>() {
                public Map doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForMap(statementName, parameterObject, keyProperty, valueProperty);
                }
            });
        }

    }

    protected Map queryForMapBase(Set<Shard> shards, SqlMapClientCallback<Map> callback) {
        List<Map> resultList = execute(shards, callback);
        Map map = new HashMap();
        for (Map m : resultList) {
            map.putAll(m);
        }
        return map;
    }

    @Override
    public Object queryForObject(String statementName) throws DataAccessException {
        return this.queryForObject(statementName, null);
    }

    @Override
    public Object queryForObject(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForObject(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForObject(statementName, parameterObject);
        } else {
            return execute(shards, new SqlMapClientCallback<Object>() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForObject(statementName, parameterObject);
                }
            });
        }
    }

    @Override
    public Object queryForObject(final String statementName, final Object parameterObject, final Object resultObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForObject(statementName, parameterObject, resultObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForObject(statementName, parameterObject, resultObject);
        } else {
            return execute(shards, new SqlMapClientCallback<Object>() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForObject(statementName, parameterObject, resultObject);
                }
            });
        }
    }

    @Override
    public void queryWithRowHandler(String statementName, RowHandler rowHandler) throws DataAccessException {
        this.queryWithRowHandler(statementName, null, rowHandler);
    }

    @Override
    public void queryWithRowHandler(final String statementName, final Object parameterObject, final RowHandler rowHandler) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) super.queryWithRowHandler(statementName, parameterObject, rowHandler);
        if (shards.size() == 1) {
            CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryWithRowHandler(statementName, parameterObject, rowHandler);
        } else {
            execute(shards, new SqlMapClientCallback<Object>() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    executor.queryWithRowHandler(statementName, parameterObject, rowHandler);
                    return null;
                }
            });
        }
    }

    @Override
    public int update(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.update(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).update(statementName, parameterObject);
        } else {
            List<Integer> resultList = execute(shards, new SqlMapClientCallback<Integer>() {
                public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.update(statementName, parameterObject);
                }
            });
            int count = 0;
            for (Integer i : resultList) {
                count += i;
            }
            return count;
        }
    }

    @Override
    public void update(String statementName, Object parameterObject, int requiredRowsAffected) throws DataAccessException {
        int actualRowsAffected = this.update(statementName, parameterObject);
        if (actualRowsAffected != requiredRowsAffected) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                    statementName, requiredRowsAffected, actualRowsAffected);
        }
    }

    @Override
    public int update(String statementName) throws DataAccessException {
        return this.update(statementName, null);
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