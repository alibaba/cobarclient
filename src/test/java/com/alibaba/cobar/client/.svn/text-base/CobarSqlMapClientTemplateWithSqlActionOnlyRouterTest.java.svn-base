package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Follower;
import com.alibaba.cobar.client.support.utils.CollectionUtils;

@Test(sequential=true)
public class CobarSqlMapClientTemplateWithSqlActionOnlyRouterTest extends
        AbstractTestNGCobarClientTest {

    public CobarSqlMapClientTemplateWithSqlActionOnlyRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/sqlaction-router-appctx.xml" });
    }

    public void testInsertOnCobarSqlMapClientWithSqlActionOnlyRules() {
        String name = "Darren";
        Follower follower = new Follower(name);
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create",
                follower);
        // since no rule for this insert, it will be inserted into default data source, that's, partition1
        String confirmSQL = "select name from followers where name='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
        // this sql action is routed to partition2, so can't find any matched record.
        Follower followerToFind = (Follower) getSqlMapClientTemplate().queryForObject(
                "com.alibaba.cobar.client.entities.Follower.finaByName", name);
        assertNull(followerToFind);
        // sql action below will be against all of the partitions , so we will get back what we want here
        @SuppressWarnings("unchecked")
        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        assertTrue(CollectionUtils.isNotEmpty(followers));
        assertEquals(1, followers.size());
        assertEquals(name, followers.get(0).getName());
    }

    public void testInsertWithBatchCommitOnCobarSqlMapClientTemplateWithSqlActionOnlyRules() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);
        // since no routing rule for insertion, all of the records will be inserted into default data source, that's, partition1
        for (String name : names) {
            String confirmSQL = "select name from followers where name='" + name + "'";
            verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
            verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
            verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
            verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
        }
        // since sql action below is routed to partition2, so no record will be found with it.
        for (String name : names) {
            Follower followerToFind = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNull(followerToFind);
        }
        // although records only reside on partition1, but we can get all of them with sql action below
        @SuppressWarnings("unchecked")
        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        assertTrue(CollectionUtils.isNotEmpty(followers));
        assertEquals(names.length, followers.size());
        for (Follower f : followers) {
            assertTrue(ArrayUtils.contains(names, f.getName()));
        }
    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
        String name = "Darren";
        String sqlAction = "com.alibaba.cobar.client.entities.Follower.deleteByName";

        // no record at beginning
        assertEquals(0, getSqlMapClientTemplate().delete(sqlAction, name));

        // insert 1 record and delete will affect this record which resides on partition1
        Follower follower = new Follower(name);
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create",
                follower);
        assertEquals(1, getSqlMapClientTemplate().delete(sqlAction, name));

        // insert 1 record to partition2, delete will NOT affect it because no rule is defined for it.
        int updatedRow = jt2m.update("insert into followers(name) values('" + name + "')");
        if (updatedRow == 1) // make sure it is do inserted into partition2 successfully.
        {
            assertEquals(0, getSqlMapClientTemplate().delete(sqlAction, name));

            @SuppressWarnings("unchecked")
            List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                    "com.alibaba.cobar.client.entities.Follower.findAll");
            assertTrue(CollectionUtils.isNotEmpty(followers));
            assertEquals(1, followers.size());
            assertEquals(name, followers.get(0).getName());
        }
    }

    /**
     * insert data onto default data source , and query will against all of the
     * partitions, so all of the records will be returned as expected.
     */
    public void testQueryForListOnCobarSqlMapClientTemplateNormally() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        @SuppressWarnings("unchecked")
        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        assertTrue(CollectionUtils.isNotEmpty(followers));
        assertEquals(names.length, followers.size());
        for (Follower f : followers) {
            assertTrue(ArrayUtils.contains(names, f.getName()));
        }
    }

    /**
     * although records are inserted onto patition2, but since the query is
     * against all of the data sources, so all of the records will be returned
     * as expected.
     */
    public void testQueryForListOnCobarSqlMapClientTemplateWithoutDefaultPartitionData() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixtureWithJdbcTemplate(names, jt2m);

        @SuppressWarnings("unchecked")
        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        assertTrue(CollectionUtils.isNotEmpty(followers));
        assertEquals(names.length, followers.size());
        for (Follower f : followers) {
            assertTrue(ArrayUtils.contains(names, f.getName()));
        }
    }

    /**
     * insert records onto partition1, but the
     * 'com.alibaba.cobar.client.entities.Follower.finaByName' will be performed
     * against partition2 as per the routing rule, so no record will be
     * returned.
     */
    public void testQueryForObjectOnCobarSqlMapClientTemplateWithDefaultPartition() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);

        for (String name : names) {
            Follower f = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNull(f);
        }
    }

    /**
     * we insert records onto partition2, and the
     * 'com.alibaba.cobar.client.entities.Follower.finaByName' action will be
     * performed against partition2 too, so each record will be returned as
     * expected.
     */
    public void testQueryForObjectOnCobarSqlMapClientTemplateWithFillingDataOntoPartition2() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixtureWithJdbcTemplate(names, jt2m);

        for (String name : names) {
            Follower f = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNotNull(f);
            assertTrue(ArrayUtils.contains(names, f.getName()));
        }
    }

    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     * <br>
     * with data fixtures setting up on default data source, and update with
     * CobarSqlMapClientTemplate.
     */
    public void testUpdateOnCobarSqlMapClientTemplateNormally() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixture(names);
        
        for(String name:names)
        {
            Follower f = (Follower)jt1m.queryForObject("select * from followers where name=?",new Object[]{name}, new RowMapper(){
                public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                    Follower fl =  new Follower();
                    fl.setId(rs.getLong(1));
                    fl.setName(rs.getString(2));
                    return fl;
                }});
            assertNotNull(f);
            int updatedCount = getSqlMapClientTemplate().update("com.alibaba.cobar.client.entities.Follower.update", f);
            assertEquals(1, updatedCount);
        }
    }
    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     * <br>
     * with data fixtures setting up on another data source, and update with
     * CobarSqlMapClientTemplate.
     */
    public void testUpdateOnCobarSqlMapClientTemplateAbnormally() {
        String[] names = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };
        batchInsertMultipleFollowersAsFixtureWithJdbcTemplate(names, jt2m);
        
        for(String name:names)
        {
            Follower f = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNotNull(f); // this sql action is performed against partition2 as per routing rule
            
            // sql action below will be performed against default data source(partition1), so will not affect any records on partition2
            int updatedCount = getSqlMapClientTemplate().update("com.alibaba.cobar.client.entities.Follower.update", f);
            assertEquals(0, updatedCount);
        }
    }
}
