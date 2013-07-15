package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Offer;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.vo.BatchInsertTask;

@Test(sequential=true)
public class CobarSqlMapClientTemplateWithComposedRuleRouterTest extends
        AbstractTestNGCobarClientTest {
    private static final String OFFER_CREATION_SQL = "com.alibaba.cobar.client.entities.Offer.create";

    public CobarSqlMapClientTemplateWithComposedRuleRouterTest() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/namespace-sqlaction-composed-router-appctx.xml" });
    }

    public void testInsertOnCobarSqlMapClientTemplate() {
        Offer offer = new Offer();
        offer.setGmtUpdated(new Date());
        offer.setMemberId(129L);
        offer.setSubject("some offer");
        Object pk = getSqlMapClientTemplate().insert(OFFER_CREATION_SQL, offer);
        assertNotNull(pk);

        String confirmSQL = "SELECT memberId FROM offers where id=" + pk;
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);

        offer = new Offer();
        offer.setGmtUpdated(new Date());
        offer.setMemberId(130L);
        offer.setSubject("some offer");
        pk = null;
        pk = getSqlMapClientTemplate().insert(OFFER_CREATION_SQL, offer);
        assertNotNull(pk);
        
        confirmSQL = "SELECT memberId FROM offers where id=" + pk;
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testInsertOnCobarSqlMapClientTemplateWithoutFoundRule() {
        Offer offer = new Offer();
        offer.setGmtUpdated(new Date());
        offer.setMemberId(128L);
        offer.setSubject("some offer");
        Object pk = getSqlMapClientTemplate().insert(OFFER_CREATION_SQL, offer);
        assertNotNull(pk);

        String confirmSQL = "SELECT memberId FROM offers where id=" + pk;
        verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
        verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
    }

    public void testBatchInsertOnCobarClientTemplateNormally() {
        Long[] memberIds = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L, 386L };
        Object pk = batchInsertOffersAsFixtureForLaterUse(memberIds);
        assertNotNull(pk);
        assertTrue(pk instanceof Collection<?>);

        for (int i = 0; i < memberIds.length; i++) {
            String confirmSQL = "select subject from offers where memberId=" + memberIds[i];
            if (i < 3) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
    }

    public void testDeleteOnCobarSqlMapClientTemplateNormally() {
        Long[] memberIds = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L, 386L };
        // 1. empty data bases
        String sqlAction = "com.alibaba.cobar.client.entities.Offer.deleteByMemberId";
        for (Long mid : memberIds) {
            Offer offer = new Offer();
            offer.setMemberId(mid);
            int affectedRows = getSqlMapClientTemplate().delete(sqlAction, offer);
            assertEquals(0, affectedRows);
        }
        // 2. insert data fixtures
        batchInsertOffersAsFixtureForLaterUse(memberIds);
        for (int i = 0; i < memberIds.length; i++) {
            String confirmSQL = "select subject from offers where memberId=" + memberIds[i];
            if (i < 3) {
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
        // 3. perform deletion and assertion
        for (Long mid : memberIds) {
            Offer offer = new Offer();
            offer.setMemberId(mid);
            int affectedRows = getSqlMapClientTemplate().delete(sqlAction, offer);
            assertEquals(1, affectedRows);
        }
    }

    public void testDeleteOnCobarSqlMapClientTemplateAbnormally() {
        Long[] memberIds = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L, 386L };
        batchInsertOffersAsFixtureForLaterUse(memberIds);

        String deleteSqlAction = "com.alibaba.cobar.client.entities.Offer.delete";
        // no rule can be found for this, so currently, it will be performed against default data source
        for (int i = 0; i < memberIds.length; i++) {
            String selectSQL = "select id from offers where memberId=" + memberIds[i];
            if (i < 3) {
                Long id = jt1m.queryForLong(selectSQL);
                int affectedRows = getSqlMapClientTemplate().delete(deleteSqlAction, id);
                assertEquals(1, affectedRows);
            } else {
                Long id = jt2m.queryForLong(selectSQL);
                int affectedRows = getSqlMapClientTemplate().delete(deleteSqlAction, id);
                assertEquals(0, affectedRows);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testQueryForListOnCobarSqlMapClientTemplateNormally() {
        Long[] memberIds = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L, 386L };
        batchInsertOffersAsFixtureForLaterUse(memberIds);

        List<Offer> offers = (List<Offer>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Offer.findAll");
        assertTrue(CollectionUtils.isNotEmpty(offers));
        assertEquals(7, offers.size());
        for (Offer offer : offers) {
            assertTrue(ArrayUtils.contains(memberIds, offer.getMemberId()));
        }

        offers = null;
        offers = (List<Offer>) getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Offer.findByMemberIdRange", 300L);
        assertTrue(CollectionUtils.isNotEmpty(offers));
        assertEquals(3, offers.size());
        Long[] partialMemberIds = new Long[] { 1L, 129L, 257L };
        for (Offer offer : offers) {
            assertTrue(ArrayUtils.contains(partialMemberIds, offer.getMemberId()));
        }
    }

    public void testQueryForObjectOnCobarSqlMapClientTemplateNormally() {
        Long[] memberIds = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L, 386L };
        batchInsertOffersAsFixtureForLaterUse(memberIds);

        // scenario 1: no routing rules are found for current sql action, so only records residing on default data source can be returned.
        String selectSqlActionOne = "com.alibaba.cobar.client.entities.Offer.load";
        for (int i = 0; i < memberIds.length; i++) {
            String confirmSQL = "select id from offers where memberId=" + memberIds[i];
            if (i < 3) {
                Long id = jt1m.queryForLong(confirmSQL);
                Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionOne,
                        id);
                assertNotNull(offer);
                assertEquals(memberIds[i], offer.getMemberId());
            } else {
                Long id = jt2m.queryForLong(confirmSQL);
                Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionOne,
                        id);
                assertNull(offer);
            }
        }
        // scenario 2: fallback sharding rules can be found for current sql action, so all requested records are returned normally.
        String selectSqlActionTwo = "com.alibaba.cobar.client.entities.Offer.findByMemberId";
        for (Long mid : memberIds) {
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionTwo,
                    parameter);
            assertNotNull(offer);
            assertEquals(mid, offer.getMemberId());
        }
    }

    public void testUpdateOnCobarSqlMapClientTemplateNormally() {
        Long[] memberIds = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L, 386L };
        batchInsertOffersAsFixtureForLaterUse(memberIds);

        String selectSqlActionTwo = "com.alibaba.cobar.client.entities.Offer.findByMemberId";
        String offerSubject = "_SUBEJCT_";
        for (Long mid : memberIds) {
            // 1. assertion before update
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionTwo,
                    parameter);
            assertNotNull(offer);
            assertEquals("fake offer", offer.getSubject());
            // 2. assertion on update
            offer.setSubject(offerSubject);
            int affectedRows = getSqlMapClientTemplate().update(
                    "com.alibaba.cobar.client.entities.Offer.update", offer);
            assertEquals(1, affectedRows);
            // 3. assertion after update
            offer = null;
            offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionTwo, parameter);
            assertNotNull(offer);
            assertEquals(offerSubject, offer.getSubject());
        }

    }

    private Object batchInsertOffersAsFixtureForLaterUse(Long[] memberIds) {
        List<Offer> offers = new ArrayList<Offer>();
        for (Long mid : memberIds) {
            Offer offer = new Offer();
            offer.setGmtUpdated(new Date());
            offer.setMemberId(mid);
            offer.setSubject("fake offer");
            offers.add(offer);
        }

        Object pk = getSqlMapClientTemplate().insert(
                "com.alibaba.cobar.client.entities.Offer.batchInsert", new BatchInsertTask(offers));
        return pk;
    }

}
