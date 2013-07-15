package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.orm.ibatis.SqlMapClientCallback;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Follower;
import com.alibaba.cobar.client.entities.Tweet;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.ibatis.sqlmap.client.SqlMapExecutor;

@Test(sequential=true)
public class CobarSqlMapClientTemplateWithNamespaceRouterTest extends AbstractTestNGCobarClientTest {

    public CobarSqlMapClientTemplateWithNamespaceRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/namespace-router-appctx.xml" });
    }

    /**
     * since {@link CobarSqlMapClientTemplate#execute(SqlMapClientCallback)},
     * {@link CobarSqlMapClientTemplate#executeWithListResult(SqlMapClientCallback)
     * )} and
     * {@link CobarSqlMapClientTemplate#executeWithMapResult(SqlMapClientCallback)
     * )} don't support partitioning behaviors, we can unit test them together
     * and use one of them as their representation.
     */
    public void testExecutePrefixMethodsOnCobarSqlMapClientTemplate() {

        getSqlMapClientTemplate().execute(new SqlMapClientCallback() {
            public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                Follower f = new Follower("fname");
                return executor.insert("com.alibaba.cobar.client.entities.Follower.create", f);
            }
        });

        String confirmSQL = "select name from followers where name='fname'";
        // execute method doesn't support partitioning behavior, so the entity will be inserted into default data source, that's , partition1, not partition2 as the rules state.
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplate() {
        Tweet t = new Tweet("tcontent");
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Tweet.create", t);

        String confirmSQL = "select tweet from tweets where tweet='tcontent'";

        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        Follower f = new Follower("fname");
         getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create", f);

        confirmSQL = "select name from followers where name='fname'";
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertInBatchOnCobarSqlMapClientTemplate() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        for (String name : names) {
            String sql = "select name from followers where name='" + name + "'";
            verifyEntityNonExistenceOnSpecificDataSource(sql, jt1m);
            verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
            verifyEntityExistenceOnSpecificDataSource(sql, jt2m);
            verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
        }
    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
        Follower f = new Follower("fname");
         getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create", f);

        String confirmSQL = "select name from followers where name='fname'";
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

         getSqlMapClientTemplate().delete("com.alibaba.cobar.client.entities.Follower.deleteByName", f);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testDeleteWithExpectedResultSizeOnCobarSqlMapClientTemplate() {
        Follower f = new Follower("fname");
         getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create", f);

        String confirmSQL = "select name from followers where name='fname'";
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        try {
             getSqlMapClientTemplate().delete("com.alibaba.cobar.client.entities.Follower.deleteByName",
                    f, 2);
            fail("only one row will be affected in fact.");
        } catch (DataAccessException e) {
            assertTrue(e instanceof JdbcUpdateAffectedIncorrectNumberOfRowsException);
            JdbcUpdateAffectedIncorrectNumberOfRowsException ex = (JdbcUpdateAffectedIncorrectNumberOfRowsException) e;
            assertEquals(1, ex.getActualRowsAffected());
        }
        // although JdbcUpdateAffectedIncorrectNumberOfRowsException is raised, but the delete does performed successfully.
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        try {
             getSqlMapClientTemplate().delete("com.alibaba.cobar.client.entities.Follower.deleteByName",
                    f, 0);
        } catch (DataAccessException e) {
            fail();
        }
    }

    public void testQueryForListOnCobarSqlMapClientTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        // 2. perform assertion
        @SuppressWarnings("unchecked")
        List<Follower> resultList = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        assertTrue(CollectionUtils.isNotEmpty(resultList));
        assertEquals(5, resultList.size());
        for (Follower f : resultList) {
            assertTrue(ArrayUtils.contains(names, f.getName()));
        }

        // 3. perform assertion with another different query
        @SuppressWarnings("unchecked")
        List<Follower> followersWithNameStartsWithA = (List<Follower>) getSqlMapClientTemplate()
                .queryForList("com.alibaba.cobar.client.entities.Follower.finaByNameAlike", "A");
        assertTrue(CollectionUtils.isNotEmpty(followersWithNameStartsWithA));
        assertEquals(3, followersWithNameStartsWithA.size());
        for (Follower f : followersWithNameStartsWithA) {
            assertTrue(ArrayUtils.contains(names, f.getName()));
        }
    }

    public void testQueryForMapOnCobarSqlMapClientTemplate() {
        // TODO low priority
    }

    /**
     * although we use queryForObject by querying on name column, but it doesn't
     * mean 'name' column is unique, it's because of the data fixture we set up
     * to use.
     */
    public void testQueryForObjectOnCobarSqlMapClientTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        // 2. assertion.
        for (String name : names) {
            Follower follower = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNotNull(follower);
        }
    }

    public void testQueryWithRowHandlerOnCobarSqlMapClientTemplate() {
        // TODO low priority
    }

    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     */
    public void testUpdateOnCobarSqlMapClientTemplate() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        String nameSuffix = "Wang";
        for (String name : names) {
            Follower follower = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNotNull(follower);
            follower.setName(follower.getName() + nameSuffix);
            getSqlMapClientTemplate().update("com.alibaba.cobar.client.entities.Follower.update",
                    follower);

            Long id = follower.getId();

            follower = null;
            follower = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNull(follower);

            follower = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.load", id);
            assertNotNull(follower);
            assertEquals(name + nameSuffix, follower.getName());
        }

    }

}
