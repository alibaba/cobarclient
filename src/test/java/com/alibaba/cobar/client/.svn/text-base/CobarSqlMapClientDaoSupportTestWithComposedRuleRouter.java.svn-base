package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Offer;

@Test(sequential = true)
public class CobarSqlMapClientDaoSupportTestWithComposedRuleRouter extends
        AbstractTestNGCobarClientTest {

    private CobarSqlMapClientDaoSupport dao        = new CobarSqlMapClientDaoSupport();
    private Long[]                      memberIds  = new Long[] { 1L, 129L, 257L, 2L, 130L, 258L,
            386L                                  };

    public static final String          CREATE_SQL = "com.alibaba.cobar.client.entities.Offer.create";
    public static final String          UPDATE_SQL = "com.alibaba.cobar.client.entities.Offer.update";
    public static final String          DELETE_SQL = "com.alibaba.cobar.client.entities.Offer.deleteByMemberId";

    public CobarSqlMapClientDaoSupportTestWithComposedRuleRouter() {
        super(new String[] { "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/namespace-sqlaction-composed-router-appctx.xml" });
    }

    @BeforeTest
    public void setupDaoSupport() {
        dao.setSqlMapClientTemplate(getSqlMapClientTemplate());
    }

    public void testBatchInsertOnDaoSupport() {
        verifyNonExistenceOnPartitions();
        List<Offer> offers = createOffersWithMemberIds(memberIds);
        int result = dao.batchInsert(CREATE_SQL, offers);
        assertEquals(7, result);
        verifyExistenceOnPartitions();
    }

    public void testBatchUpdateOnDaoSupport() {
        verifyNonExistenceOnPartitions();
        List<Offer> offers = createOffersWithMemberIds(memberIds);

        int updatedNumber = dao.batchUpdate(UPDATE_SQL, offers);
        assertEquals(0, updatedNumber);

        int result = dao.batchInsert(CREATE_SQL, offers);
        assertEquals(7, result);
        verifyExistenceOnPartitions();

        for (Offer offer : offers) {
            offer.setSubject("_subject_to_update_");
        }

        updatedNumber = dao.batchUpdate(UPDATE_SQL, offers);
        assertEquals(7, updatedNumber);
    }

    public void testBatchDeleteOnDaoSupport() {
        verifyNonExistenceOnPartitions();

        List<Offer> offers = createOffersWithMemberIds(memberIds);
        int result = dao.batchDelete(DELETE_SQL, offers);
        assertEquals(0, result);

        int insertCount = dao.batchInsert(CREATE_SQL, offers);
        assertEquals(7, insertCount);
        verifyExistenceOnPartitions();
        
        int deleteCount = dao.batchDelete(DELETE_SQL, offers);
        assertEquals(7, deleteCount);
        verifyNonExistenceOnPartitions();
    }

    private List<Offer> createOffersWithMemberIds(Long[] memberIds) {
        List<Offer> offers = new ArrayList<Offer>();
        for (Long mid : memberIds) {
            Offer offer = new Offer();
            offer.setGmtUpdated(new Date());
            offer.setMemberId(mid);
            offer.setSubject("fake offer");
            offers.add(offer);
        }
        return offers;
    }

    private void verifyNonExistenceOnPartitions() {
        for (int i = 0; i < memberIds.length; i++) {
            String confirmSQL = "select subject from offers where memberId=" + memberIds[i];
            if (i < 3) {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            } else {
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt1s);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2m);
                verifyEntityNonExistenceOnSpecificDataSource(confirmSQL, jt2s);
            }
        }
    }

    private void verifyExistenceOnPartitions() {
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
}
