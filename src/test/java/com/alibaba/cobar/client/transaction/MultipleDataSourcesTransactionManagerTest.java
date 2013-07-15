package com.alibaba.cobar.client.transaction;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.AbstractTestNGCobarClientTest;
import com.alibaba.cobar.client.entities.Offer;
import com.alibaba.cobar.client.test.services.IOfferService;

/**
 * H2 In-Memory Database doesn't support transaction, so in this test case, we
 * need to turn to non-in-memory database to test the transaction.<br>
 * 
 * @author fujohnwang
 */
@Test(sequential=true)
public class MultipleDataSourcesTransactionManagerTest extends AbstractTestNGCobarClientTest {

    String                       selectSqlActionTwo    = "com.alibaba.cobar.client.entities.Offer.findByMemberId";

    private Long[]               memberIds             = new Long[] { 1L, 129L, 257L, 2L, 130L,
            258L, 386L                                };

    public MultipleDataSourcesTransactionManagerTest() {
        super(new String[] {
                "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/namespace-sqlaction-composed-router-appctx.xml",
                "META-INF/spring/cobar-client-offer-services-appctx.xml" });
    }

    public void testOfferCreationOnMultipleShardsWithTransactionRollback() {

        new TransactionTemplate(((PlatformTransactionManager) getApplicationContext()
                .getBean("transactionManager"))).execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {

                try {
                    Offer offer = new Offer();
                    offer.setMemberId(1L);
                    offer.setGmtUpdated(new Date());
                    offer.setSubject("o1");
                    getSqlMapClientTemplate().insert(
                            "com.alibaba.cobar.client.entities.Offer.create", offer);

                    offer = new Offer();
                    offer.setMemberId(2L);
                    offer.setGmtUpdated(new Date());
                    offer.setSubject("o2");
                    getSqlMapClientTemplate().insert(
                            "com.alibaba.cobar.client.entities.Offer.create", offer);

                } finally {
                    status.setRollbackOnly();
                }

            }
        });

        Long[] mids = new Long[] { 1L, 2L };
        for (Long mid : mids) {
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            assertNull(getSqlMapClientTemplate().queryForObject(selectSqlActionTwo, parameter));
        }
    }

    public void testOfferCreationOnMultipleShardsWithNormallyOfferService() {
        String selectSqlActionTwo = "com.alibaba.cobar.client.entities.Offer.findByMemberId";

        for (Long mid : memberIds) {
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionTwo,
                    parameter);
            assertNull(offer);
        }

        ((IOfferService) getApplicationContext().getBean("normalOfferService"))
                .createOffersInBatch(createOffersWithMemberIdsFrom(memberIds));

        for (Long mid : memberIds) {
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionTwo,
                    parameter);
            assertNotNull(offer);
            assertEquals(mid, offer.getMemberId());
        }
    }

    /**
     * need data stores that support transaction to test this behavior.
     */
    public void testOfferCreationOnMultipleShardsWithAbnormalOfferService() {
        String selectSqlActionTwo = "com.alibaba.cobar.client.entities.Offer.findByMemberId";

        for (Long mid : memberIds) {
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            Offer offer = (Offer) getSqlMapClientTemplate().queryForObject(selectSqlActionTwo,
                    parameter);
            assertNull(offer);
        }

        try {
            Object offerService = getApplicationContext().getBean("abnormalOfferService");
            assertTrue(offerService instanceof Proxy);
            ((IOfferService) offerService)
                    .createOffersInBatch(createOffersWithMemberIdsFrom(memberIds));
            fail();
        } catch (RuntimeException e) {
            // pass
        }

        for (Long mid : memberIds) {
            Offer parameter = new Offer();
            parameter.setMemberId(mid);
            assertNull(getSqlMapClientTemplate().queryForObject(selectSqlActionTwo, parameter));
        }
    }

    public List<Offer> createOffersWithMemberIdsFrom(Long[] mids) {
        List<Offer> offers = new ArrayList<Offer>();
        for (Long mid : mids) {
            Offer offer = new Offer();
            offer.setGmtUpdated(new Date());
            offer.setMemberId(mid);
            offer.setSubject("anything");
            offers.add(offer);
        }
        return offers;
    }
}
