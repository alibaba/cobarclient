package com.alibaba.cobarclient;

import com.alibaba.cobarclient.route.Router;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.orm.ibatis.SqlMapClientTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

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
}
