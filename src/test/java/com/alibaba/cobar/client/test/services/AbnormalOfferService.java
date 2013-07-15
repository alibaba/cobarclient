package com.alibaba.cobar.client.test.services;

import java.util.List;

import org.springframework.transaction.annotation.Transactional;

import com.alibaba.cobar.client.entities.Offer;
import com.alibaba.cobar.client.support.vo.BatchInsertTask;

public class AbnormalOfferService extends NormalOfferService {
    @Transactional
    public void createOffersInBatch(List<Offer> offers) {
        getSqlMapClientTemplate().insert("com.alibaba.cobar.client.entities.Offer.batchInsert", new BatchInsertTask(offers));
        throw new RuntimeException("exception to trigger rollback");
    }

}
