package com.alibaba.cobar.client;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.springframework.orm.ibatis.SqlMapClientTemplate;
import org.testng.annotations.Test;

import com.alibaba.cobar.client.entities.Offer;
import com.alibaba.cobar.client.support.utils.CollectionUtils;
import com.alibaba.cobar.client.support.vo.BatchInsertTask;

public class CobarSqlMapClientTemplateWithCustomMergerTest extends AbstractTestNGCobarClientTest {

    public CobarSqlMapClientTemplateWithCustomMergerTest() {
        super(new String[] { "META-INF/spring/datasources-appctx.xml",
                "META-INF/spring/cobar-client-appctx.xml",
                "META-INF/spring/cobar-client-custom-merger-appctx.xml" });
    }

    @Test
    public void testQueryForListWithCustomMerger() {
        batchInsertOffersAsFixture();

        SqlMapClientTemplate st = (SqlMapClientTemplate) getApplicationContext().getBean(
                "sqlMapClientTemplateWithMerger");
        @SuppressWarnings("unchecked")
        List lst = st
                .queryForList("com.alibaba.cobar.client.entities.Offer.findAllWithOrderByOnSubject");
        assertTrue(CollectionUtils.isNotEmpty(lst));
        assertEquals(5, lst.size());

        verifyOffersOrderBySubject(lst);

    }

    @SuppressWarnings("unchecked")
    private void verifyOffersOrderBySubject(List lst) {
        for (int i = 0; i < lst.size(); i++) {
            Offer offer = (Offer) lst.get(i);
            if (i == 0 || i == 1) {
                assertEquals(2, offer.getMemberId().longValue());
            } else {
                assertEquals(1, offer.getMemberId().longValue());
            }
            switch (i) {
                case 0:
                    assertEquals("A", offer.getSubject());
                    break;
                case 1:
                    assertEquals("D", offer.getSubject());
                    break;
                case 2:
                    assertEquals("S", offer.getSubject());
                    break;
                case 3:
                    assertEquals("X", offer.getSubject());
                    break;
                case 4:
                    assertEquals("Z", offer.getSubject());
                    break;
                default:
                    throw new IllegalArgumentException("unexpected condition.");
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryForListWithoutCustomMerger() {
        batchInsertOffersAsFixture();

        List lst = getSqlMapClientTemplate().queryForList(
                "com.alibaba.cobar.client.entities.Offer.findAllWithOrderByOnSubject");

        assertTrue(CollectionUtils.isNotEmpty(lst));
        // contains all of the entities, but the order is not guaranteed.
        assertEquals(5, lst.size());

        // sort in application code
        Comparator<Offer> comparator = (Comparator<Offer>) getApplicationContext().getBean(
                "comparator");
        Collections.sort(lst, comparator);
        verifyOffersOrderBySubject(lst);
    }

    private void batchInsertOffersAsFixture() {
        BatchInsertTask task = new BatchInsertTask();

        List<Offer> offers = new ArrayList<Offer>();
        Offer offer = new Offer();
        offer.setMemberId(1L);
        offer.setSubject("Z");
        offer.setGmtUpdated(new Date());
        offers.add(offer);

        offer = new Offer();
        offer.setMemberId(1L);
        offer.setSubject("X");
        offer.setGmtUpdated(new Date());
        offers.add(offer);

        offer = new Offer();
        offer.setMemberId(1L);
        offer.setSubject("S");
        offer.setGmtUpdated(new Date());
        offers.add(offer);

        offer = new Offer();
        offer.setMemberId(2L);
        offer.setSubject("D");
        offer.setGmtUpdated(new Date());
        offers.add(offer);

        offer = new Offer();
        offer.setMemberId(2L);
        offer.setSubject("A");
        offer.setGmtUpdated(new Date());
        offers.add(offer);

        task.setEntities(offers);

        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Offer.batchInsert",
                task);
    }
}
