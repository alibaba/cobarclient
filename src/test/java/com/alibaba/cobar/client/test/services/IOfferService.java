package com.alibaba.cobar.client.test.services;

import java.util.List;

import com.alibaba.cobar.client.entities.Offer;

public interface IOfferService {
    
    void createOffersInBatch(List<Offer> offers);
    
}
