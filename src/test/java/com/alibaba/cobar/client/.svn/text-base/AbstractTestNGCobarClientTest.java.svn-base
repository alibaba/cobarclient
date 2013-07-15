package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.ibatis.SqlMapClientTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;

import com.alibaba.cobar.client.entities.Follower;
import com.alibaba.cobar.client.support.vo.BatchInsertTask;

public abstract class AbstractTestNGCobarClientTest {
    
    private transient final Logger logger = LoggerFactory
            .getLogger(AbstractTestNGCobarClientTest.class);
    
    public static final String   CREATE_TABLE_COBAR_HA    = "CREATE TABLE IF NOT EXISTS cobarha(timeflag TIMESTAMP)";
    public static final String   CREATE_TABLE_TWEETS      = "CREATE TABLE IF NOT EXISTS tweets(id BIGINT IDENTITY PRIMARY KEY, tweet VARCHAR(140))";
    public static final String   CREATE_TABLE_FOLLOWERS   = "CREATE TABLE IF NOT EXISTS followers(id BIGINT IDENTITY PRIMARY KEY,name VARCHAR(255))";
    public static final String   CREATE_TABLE_OFFERS      = "CREATE TABLE IF NOT EXISTS offers(id BIGINT(20) AUTO_INCREMENT PRIMARY KEY, memberId VARCHAR(32), subject  VARCHAR(512), gmtUpdated TIMESTAMP default CURRENT_TIMESTAMP())";

    public static final String   TRUNCATE_TABLE_COBARHA   = "TRUNCATE TABLE cobarha";
    public static final String   TRUNCATE_TABLE_TWEETS    = "TRUNCATE TABLE tweets";
    public static final String   TRUNCATE_TABLE_FOLLOWERS = "TRUNCATE TABLE followers";
    public static final String   TRUNCATE_TABLE_OFFERS    = "TRUNCATE TABLE offers";

    private ApplicationContext   applicationContext;
    private SqlMapClientTemplate sqlMapClientTemplate;
    protected JdbcTemplate       jt1m;
    protected JdbcTemplate       jt1s;
    protected JdbcTemplate       jt2m;
    protected JdbcTemplate       jt2s;

    public AbstractTestNGCobarClientTest(String[] locations) {
        applicationContext = new ClassPathXmlApplicationContext(locations);
        ((AbstractApplicationContext) applicationContext).registerShutdownHook();

        setSqlMapClientTemplate((SqlMapClientTemplate) applicationContext
                .getBean("sqlMapClientTemplate"));

        jt1m = new JdbcTemplate((DataSource) applicationContext.getBean("partition1_main"));
        jt1s = new JdbcTemplate((DataSource) applicationContext.getBean("partition1_standby"));
        jt2m = new JdbcTemplate((DataSource) applicationContext.getBean("partition2_main"));
        jt2s = new JdbcTemplate((DataSource) applicationContext.getBean("partition2_standby"));

        jt1m.execute(CREATE_TABLE_COBAR_HA);
        jt1m.execute(CREATE_TABLE_TWEETS);
        jt1m.execute(CREATE_TABLE_FOLLOWERS);
        jt1m.execute(CREATE_TABLE_OFFERS);

        jt1s.execute(CREATE_TABLE_COBAR_HA);
        jt1s.execute(CREATE_TABLE_TWEETS);
        jt1s.execute(CREATE_TABLE_FOLLOWERS);
        jt1s.execute(CREATE_TABLE_OFFERS);

        jt2m.execute(CREATE_TABLE_COBAR_HA);
        jt2m.execute(CREATE_TABLE_FOLLOWERS);
        jt2m.execute(CREATE_TABLE_TWEETS);
        jt2m.execute(CREATE_TABLE_OFFERS);

        jt2s.execute(CREATE_TABLE_COBAR_HA);
        jt2s.execute(CREATE_TABLE_FOLLOWERS);
        jt2s.execute(CREATE_TABLE_TWEETS);
        jt2s.execute(CREATE_TABLE_OFFERS);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUpBeforeEachTestMethodRun() {
        jt1m.execute(TRUNCATE_TABLE_COBARHA);
        jt1m.execute(TRUNCATE_TABLE_TWEETS);
        jt1m.execute(TRUNCATE_TABLE_FOLLOWERS);
        jt1m.execute(TRUNCATE_TABLE_OFFERS);

        jt1s.execute(TRUNCATE_TABLE_COBARHA);
        jt1s.execute(TRUNCATE_TABLE_TWEETS);
        jt1s.execute(TRUNCATE_TABLE_FOLLOWERS);
        jt1s.execute(TRUNCATE_TABLE_OFFERS);

        jt2m.execute(TRUNCATE_TABLE_COBARHA);
        jt2m.execute(TRUNCATE_TABLE_FOLLOWERS);
        jt2m.execute(TRUNCATE_TABLE_TWEETS);
        jt2m.execute(TRUNCATE_TABLE_OFFERS);

        jt2s.execute(TRUNCATE_TABLE_COBARHA);
        jt2s.execute(TRUNCATE_TABLE_FOLLOWERS);
        jt2s.execute(TRUNCATE_TABLE_TWEETS);
        jt2s.execute(TRUNCATE_TABLE_OFFERS);
    }

    @AfterClass
    public void cleanup() {
        if (applicationContext != null) {
            logger.info("shut down Application Context to clean up.");            
            ((AbstractApplicationContext) applicationContext).destroy();
        }
    }

    protected void batchInsertMultipleFollowersAsFixture(String[] names) {
        List<Follower> followers = new ArrayList<Follower>();
        for (String name : names) {
            followers.add(new Follower(name));
        }

        BatchInsertTask task = new BatchInsertTask(followers);
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.batchInsert",
                task);
    }

    protected void batchInsertMultipleFollowersAsFixtureWithJdbcTemplate(String[] names,
                                                                         JdbcTemplate jt) {
        for (String name : names) {
            String sql = "insert into followers(name) values('" + name + "')";
            jt.update(sql);
        }
    }

    protected void verifyEntityNonExistenceOnSpecificDataSource(String sql, JdbcTemplate jt) {
        try {
            jt.queryForObject(sql, String.class);
            fail();
        } catch (DataAccessException e) {
            // pass
        }
    }

    protected void verifyEntityExistenceOnSpecificDataSource(String sql, JdbcTemplate jt) {
        try {
            assertNotNull(jt.queryForObject(sql, String.class));
        } catch (DataAccessException e) {
            fail();
        }
    }

    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public void setSqlMapClientTemplate(SqlMapClientTemplate sqlMapClientTemplate) {
        this.sqlMapClientTemplate = sqlMapClientTemplate;
    }

    public SqlMapClientTemplate getSqlMapClientTemplate() {
        return sqlMapClientTemplate;
    }
}
