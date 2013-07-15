package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Follower;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
@Test(sequential=true)
public class CobarSqlMapClientTemplateWithSqlActionShardingRouterTest extends
        AbstractTestNGCobarClientTest {

    private transient final Logger logger = LoggerFactory
                                                  .getLogger(CobarSqlMapClientTemplateWithSqlActionShardingRouterTest.class);

    private String[]               names  = { "Aaron", "Amily", "Aragon", "Darren", "Darwin" };

    public CobarSqlMapClientTemplateWithSqlActionShardingRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/sqlaction-sharding-router-appctx.xml" });
    }
    
    public void testInsertOnCobarSqlMapClientTemplateWithFollowerA() {
        String name = "Aranda";
        Follower follower = new Follower(name);
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create",
                follower);

        String confirmSQL = "select name from followers where name='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplateWithFollowerD() {
        String name = "Darl";
        Follower follower = new Follower(name);
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create",
                follower);

        String confirmSQL = "select name from followers where name='" + name + "'";
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplateWithFollowerNonAorD() {
        String name = "Sara";
        Follower follower = new Follower(name);
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Follower.create",
                follower);

        String confirmSQL = "select name from followers where name='" + name + "'";
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertWithBatchOnCobarSqlMapClientTemplate() {
        batchInsertMultipleFollowersAsFixture(names);
        for (String name : names) {
            String confirmSQL = "select name from followers where name='" + name + "'";
            if (name.startsWith("A")) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
            if (name.startsWith("D")) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
        @SuppressWarnings("unchecked")
        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        // since no routing rule is defined for 'findAll', it can only find out records on default data source
        assertTrue(CollectionUtils.isNotEmpty(followers));
        assertEquals(3, followers.size());
        for (Follower follower : followers) {
            assertTrue(ArrayUtils.contains(names, follower.getName()));
        }

        assertEquals(2, jt2m.queryForInt("select count(1) from followers"));
    }

    public void testInsertWithBatchOnCobarSqlMapClientTemplateWithDataHavingNoRuleQualifiedFor() {
        List<String> nameList = new ArrayList<String>();
        nameList.addAll(Arrays.asList(names));
        nameList.add("Sara");
        nameList.add("Samansa");
        batchInsertMultipleFollowersAsFixture(nameList.toArray(new String[nameList.size()]));

        for (String name : nameList) {
            String confirmSQL = "select name from followers where name='" + name + "'";
            if (name.startsWith("D")) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }

        @SuppressWarnings("unchecked")
        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        // since 'findAll' action will be performed only against default data source(partition1), 
        // it will only find records whose 'name' column has value that doesn't start with 'D'
        assertTrue(CollectionUtils.isNotEmpty(followers));
        assertEquals(5, followers.size());
        for (Follower follower : followers) {
            assertTrue(nameList.contains(follower.getName()));
        }

        assertEquals(2, jt2m.queryForInt("select count(1) from followers"));
    }

    public void testDeleteOnCobarSqlMapClientTemplate() {
        batchInsertMultipleFollowersAsFixture(names);
        for (String name : names) {
            String confirmSQL = "select name from followers where name='" + name + "'";
            if (name.startsWith("A")) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
            if (name.startsWith("D")) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }

        // no rules are defined for deletion, so it will be only performed on default data source(partition1).
        for (String name : names) {
            int affectedCount = getSqlMapClientTemplate().delete(
                    "com.alibaba.cobar.client.entities.Follower.deleteByName", name);
            if (name.startsWith("A")) {
                assertEquals(1, affectedCount);
            }
            if (name.startsWith("D")) {
                assertEquals(0, affectedCount);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testQueryForListOnCobarSqlMapClientTemplateWithPartialDataOnDefaultDataSource() {
        batchInsertMultipleFollowersAsFixture(names);

        List<Follower> followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.findAll");
        assertTrue(CollectionUtils.isNotEmpty(followers));
        // only followers with name that starts with "A" can be found
        assertEquals(3, followers.size());
        for (Follower follower : followers) {
            assertTrue(follower.getName().startsWith("A"));
        }

        followers = null;
        followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.finaByNameAlike", "A");
        assertTrue(CollectionUtils.isNotEmpty(followers));
        // only followers with name that starts with "A" can be found
        assertEquals(3, followers.size());
        for (Follower follower : followers) {
            assertTrue(follower.getName().startsWith("A"));
        }

        followers = null;
        followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.finaByNameAlike", "D");
        assertTrue(CollectionUtils.isEmpty(followers));

        followers = null;
        followers = (List<Follower>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Follower.finaByNameAlike", "S");
        assertTrue(CollectionUtils.isEmpty(followers));
    }

    public void testQueryForObjectOnCobarSqlMapClientTemplate() {
        batchInsertMultipleFollowersAsFixture(names);

        for (String name : names) {
            Follower follower = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            if (name.startsWith("A")) {
                assertNotNull(follower);
            }
            if (name.startsWith("D")) {
                assertNull(follower);
            }
        }

        String name = "Jesus";
        int count = jt2m.update("insert into followers(name) values(?)", new Object[] { name });
        if (count == 1) {
            Follower f = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNull(f);
        }

        Follower follower = new Follower(name);
        Object pk = getSqlMapClientTemplate().insert(
                "com.alibaba.cobar.client.entities.Follower.create", follower);
        if (pk != null) {
            Follower f = (Follower) getSqlMapClientTemplate().queryForObject(
                    "com.alibaba.cobar.client.entities.Follower.finaByName", name);
            assertNotNull(f);
            assertEquals(name, f.getName());
        } else {
            logger.warn("failed to create fixture Follower object.");
        }
    }
    /**
     * WARNING: don't do stupid things such like below, we do this because we
     * can guarantee the shard id will NOT change. if you want to use cobar
     * client corretly, make sure you are partitioning you databases with shard
     * id that will not be changed once it's created!!!
     */
    public void testUpdateOnCobarSqlMapClientTemplate(){
        batchInsertMultipleFollowersAsFixture(names);
        
        List<Follower> followersToUpdate = new ArrayList<Follower>();
        for(String name:names)
        {
            Follower follower = (Follower)getSqlMapClientTemplate().queryForObject("com.alibaba.cobar.client.entities.Follower.finaByName", name);
            if(follower != null)
            {
                followersToUpdate.add(follower);
            }
            else
            {
                follower = (Follower)jt2m.queryForObject("select * from followers where name=?", new Object[]{name}, new RowMapper(){
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Follower f = new Follower();
                        f.setId(rs.getLong(1));
                        f.setName(rs.getString(2));
                        return f;
                    }});
                followersToUpdate.add(follower);
            }
        }
        assertEquals(5, followersToUpdate.size());
        
        for(Follower follower:followersToUpdate)
        {
            follower.setName(follower.getName().toUpperCase());
            int affectedRows = getSqlMapClientTemplate().update("com.alibaba.cobar.client.entities.Follower.update", follower);
            if(follower.getName().startsWith("A"))
            {
                assertEquals(1, affectedRows);
                Follower f = (Follower)getSqlMapClientTemplate().queryForObject("com.alibaba.cobar.client.entities.Follower.load",follower.getId());
                assertEquals(follower.getName(), f.getName());
            }
            else
            {
                assertEquals(0, affectedRows);
            }
        }
        
        
    }
}
