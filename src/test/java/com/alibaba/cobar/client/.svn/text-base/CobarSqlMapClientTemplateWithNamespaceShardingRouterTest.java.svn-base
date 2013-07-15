package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Follower;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.vo.BatchInsertTask;

@Test(sequential=true)
public class CobarSqlMapClientTemplateWithNamespaceShardingRouterTest extends
        AbstractTestNGCobarClientTest {

    private String                 insertSQLAction      = "com.alibaba.cobar.client.entities.Follower.create";
    private String                 batchInsertSQLAction = "com.alibaba.cobar.client.entities.Follower.batchInsert";

    public CobarSqlMapClientTemplateWithNamespaceShardingRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/namespace-sharding-router-appctx.xml" });
    }

    public void testInsertOnCobarSqlMapClientTemplateWithSingleEntityNormally() {

        String name = "Darren"; // shard standard
        Follower f = new Follower(name);
        getSqlMapClientTemplate().insert(insertSQLAction, f);

        String sql = "select name from followers where name='" + name + "'";
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
        verifyEntityExistenceOnSpecificDataSource(sql, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);

        name = "Aaron";
        f = new Follower(name);
        getSqlMapClientTemplate().insert(insertSQLAction, f);

        sql = "select name from followers where name='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(sql, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplateWithMultipleEntities() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };

        List<Follower> followers = new ArrayList<Follower>();
        for (String name : names) {
            followers.add(new Follower(name));
        }
        /**
         * NOTE: if the sqlmap is drafted with invalid format, data access
         * exception will be raised, usually, the information of exception
         * doesn't tell too much.
         */
        getSqlMapClientTemplate().insert(batchInsertSQLAction, new BatchInsertTask(followers));

        for (String name : names) {
            String sql = "select name from followers where name='" + name + "'";
            if (name.startsWith("A")) {
                verifyEntityExistenceOnSpecificDataSource(sql, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt1s);
                verifyEntityExistenceOnSpecificDataSource(sql, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(sql, jt2s);
            }

        }
    }

    /**
     * if no rule is found for current data access request, the data access
     * request will be performed on default data source, that's, partition1.
     */
    public void testInsertWithoutFindingRuleOnCobarSqlMapClientTemplate() {
        String nameStartsWithS = "Sara";
        Follower follower = new Follower(nameStartsWithS);
        getSqlMapClientTemplate().insert(insertSQLAction, follower);

        String confirmSQL = "select name from followers where name='" + nameStartsWithS + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
        Follower f = new Follower("Darren");
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create", f);

        String confirmSQL = "select name from followers where name='Darren'";
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

    /**
     * MARK!!!
     */
    public void testDeleteWithExpectedResultSizeOnCobarSqlMapClientTemplate() {
        Follower f = new Follower("Darren");
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create", f);

        String confirmSQL = "select name from followers where name='Darren'";
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

        f = new Follower("Amanda");
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
        assertEquals(3, resultList.size()); // no rule match 'findAll', so query is performed against default data source - partition1
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

        @SuppressWarnings("unchecked")
        List<Follower> followersWithNameStartsWithD = (List<Follower>) getSqlMapClientTemplate()
                .queryForList("com.alibaba.cobar.client.entities.Follower.finaByNameAlike", "D");
        assertTrue(CollectionUtils.isEmpty(followersWithNameStartsWithD));
    }

    /**
     * adapted from {@link CobarSqlMapClientTemplateWithNamespaceRouterTest}
     * with some adjustments.
     */
    public void testQueryForObjectOnCobarSqlMapClientTemplate() {
        // 1. initialize data
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        // 2. assertion.
        for (String name : names) {
            Follower follower = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            if (name.startsWith("A")) {
                assertNotNull(follower);
            } else {
                assertNull(follower);
            }
        }
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
            if (name.startsWith("A")) {
                Follower follower = (Follower) getSqlMapClientTemplate().queryForObject(
                        "com.alibaba.cobar.client.entities.Follower.finaByName", name);
                assertNotNull(follower);
                follower.setName(follower.getName() + nameSuffix);
                getSqlMapClientTemplate().update(
                        "com.alibaba.cobar.client.entities.Follower.update", follower);

                Long id = follower.getId();

                follower = null;
                follower = (Follower) getSqlMapClientTemplate().queryForObject(
                        "com.alibaba.cobar.client.entities.Follower.finaByName", name);
                assertNull(follower);

                follower = (Follower) getSqlMapClientTemplate().queryForObject(
                        "com.alibaba.cobar.client.entities.Follower.load", id);
                assertNotNull(follower);
                assertEquals(name + nameSuffix, follower.getName());
            } else {
                String sql = "select * from followers where name=?";
                RowMapper rowMapper = new RowMapper() {
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Follower f = new Follower();
                        f.setId(rs.getLong(1));
                        f.setName(rs.getString(2));
                        return f;
                    }
                };
                Follower follower = (Follower) jt2m.queryForObject(sql, new Object[] { name },
                        rowMapper);
                assertNotNull(follower);
                follower.setName(follower.getName() + nameSuffix);
                getSqlMapClientTemplate().update(
                        "com.alibaba.cobar.client.entities.Follower.update", follower);

                Long id = follower.getId();

                follower = null;
                try {
                    follower = (Follower) jt2m
                            .queryForObject(sql, new Object[] { name }, rowMapper);
                    fail();
                } catch (DataAccessException e) {
                    assertTrue(e instanceof EmptyResultDataAccessException);
                }

                follower = (Follower) jt2m.queryForObject("select * from followers where id=?",
                        new Object[] { id }, rowMapper);
                assertNotNull(follower);
                assertEquals(name + nameSuffix, follower.getName());
            }
        }
    }

}
