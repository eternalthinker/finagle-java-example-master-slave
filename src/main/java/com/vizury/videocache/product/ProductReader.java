/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.product;

import java.util.List;

/**
 *
 * @author sankalpkulshrestha
 */
public interface ProductReader {
    public List<ProductDetail> getProductDetailList(String campaignId);
    public String[] getCampaignList();
}
