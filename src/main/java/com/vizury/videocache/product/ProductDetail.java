/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.product;

import com.vizury.videocache.core.CacheConnect;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author sankalpkulshrestha
 */
public class ProductDetail {

    private String namespace;
    private int segmentId;
    private int bannerClassId;
    private String productName;
    private String productId;
    private String cdnUrl;
    private String landingPageUrl;
    private String categoryId;
    private String subCategoryId;
    private String subSubCategoryId;
    private ProductDetail[] recommendedProduct;
    private boolean validProduct;

    public ProductDetail(int segmentId, int bannerClassId, String productId) {
        this.segmentId = segmentId;
        this.bannerClassId = bannerClassId;
        this.productId = productId;
    }

    private ProductDetail(String productId, String namespace) {
        this.productId = productId;
        this.namespace = namespace;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        result.append(this.getClass().getName());
        result.append(" Object {");
        result.append(newLine);

        //determine fields declared in this class only (no fields of superclass)
        Field[] fields = this.getClass().getDeclaredFields();

        //print field names paired with their values
        for (Field field : fields) {
            result.append("  ");
            try {
                result.append(field.getName());
                result.append(": ");
                //requires access to private field:
                if (field.getName().equals("recommendedProduct")) {
                    if (recommendedProduct != null) {
                        for (ProductDetail p : recommendedProduct) {
                            result.append(p.toString());
//                            System.out.println(p.getProductId());
//                            System.out.println(p.getProductName());
//                            System.out.println(p.getLandingPageUrl());
//                            System.out.println(p.getCdnUrl());
                        }
                    }
                } else {
                    result.append(field.get(this));
                }
            } catch (IllegalAccessException ex) {
                System.out.println(ex);
            }
            result.append(newLine);
        }
        result.append("}");

        return result.toString();
    }

    /**
     * @return the segmentId
     */
    public int getSegmentId() {
        return segmentId;
    }

    /**
     * @param segmentId the segmentId to set
     */
    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    /**
     * @return the productId
     */
    public String getProductId() {
        return productId;
    }

    /**
     * @param productId the productId to set
     */
    public void setProductId(String productId) {
        this.productId = productId;
    }

    /**
     * @return the bannerClassId
     */
    public int getBannerClassId() {
        return bannerClassId;
    }

    /**
     * @param bannerClassId the bannerClassId to set
     */
    public void setBannerClassId(int bannerClassId) {
        this.bannerClassId = bannerClassId;
    }

    /**
     * @return the cdnUrl
     */
    public String getCdnUrl() {
        return cdnUrl;
    }

    /**
     * @param cdnUrl the cdnUrl to set
     */
    public void setCdnUrl(String cdnUrl) {
        this.cdnUrl = cdnUrl;
    }

    /**
     * @return the landingPageUrl
     */
    public String getLandingPageUrl() {
        return landingPageUrl;
    }

    /**
     * @param landingPageUrl the landingPageUrl to set
     */
    public void setLandingPageUrl(String landingPageUrl) {
        this.landingPageUrl = landingPageUrl;
    }

    /**
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @param namespace the namespace to set
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * @return the productName
     */
    public String getProductName() {
        return productName;
    }

    /**
     * @param productName the productName to set
     */
    public void setProductName(String productName) {
        this.productName = productName;
    }

    /**
     * @return the categoryId
     */
    public String getCategoryId() {
        return categoryId;
    }

    /**
     * @param categoryId the categoryId to set
     */
    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    /**
     * @return the subCategoryId
     */
    public String getSubCategoryId() {
        return subCategoryId;
    }

    /**
     * @param subCategoryId the subCategoryId to set
     */
    public void setSubCategoryId(String subCategoryId) {
        this.subCategoryId = subCategoryId;
    }

    /**
     * @return the subSubCategoryId
     */
    public String getSubSubCategoryId() {
        return subSubCategoryId;
    }

    /**
     * @param subSubCategoryId the subSubCategoryId to set
     */
    public void setSubSubCategoryId(String subSubCategoryId) {
        this.subSubCategoryId = subSubCategoryId;
    }

    public void getRecommendedProducts(CacheConnect cache, HashMap<String, String> subSubCategoryMap, HashMap<String, String> subCategoryMap, HashMap<String, String> categoryMap, HashMap<String, ProductDetail> recommendedProductDetail, int numberOfRecommendedProducts) {
        ProductDetail[] subSubCategoryProductList = getSubSubCategoryProductList(cache, subSubCategoryMap, recommendedProductDetail, numberOfRecommendedProducts);
        ProductDetail[] subCategoryProductList = null;
        ProductDetail[] categoryProductList = null;
        recommendedProduct = new ProductDetail[numberOfRecommendedProducts];
        int subSubCategoryProductListLength = (subSubCategoryProductList != null) ? subSubCategoryProductList.length : 0;
        int subCategoryProductListLength = 0;
        int categoryProductListLength = 0;
        setValidProduct(true);
        if (subSubCategoryProductListLength < numberOfRecommendedProducts) {
            subCategoryProductList = getSubCategoryProductList(cache, subCategoryMap, recommendedProductDetail, numberOfRecommendedProducts - subSubCategoryProductListLength);
            subCategoryProductListLength = (subCategoryProductList != null) ? subCategoryProductList.length : 0;
            if (subCategoryProductListLength + subSubCategoryProductListLength < numberOfRecommendedProducts) {
                categoryProductList = getCategoryProductList(cache, categoryMap, recommendedProductDetail, numberOfRecommendedProducts - subSubCategoryProductListLength - subCategoryProductListLength);
                categoryProductListLength = (categoryProductList != null) ? categoryProductList.length : 0;
                if (categoryProductListLength + subCategoryProductListLength + subSubCategoryProductListLength < numberOfRecommendedProducts) {
                    setValidProduct(false);
                }
            }
        }
        if (isValidProduct()) {
            int index = 0;
            int subIndex;
            if (subSubCategoryProductListLength != 0) {
                for (subIndex = 0; subIndex < subSubCategoryProductListLength; subIndex++) {
                    recommendedProduct[index] = subSubCategoryProductList[subIndex];
                    index++;
                }
            }
            if (subCategoryProductListLength != 0) {
                for (subIndex = 0; subIndex < subCategoryProductListLength; subIndex++) {
                    recommendedProduct[index] = subCategoryProductList[subIndex];
                    index++;
                }
            }
            if (categoryProductListLength != 0) {
                for (subIndex = 0; subIndex < categoryProductListLength; subIndex++) {
                    recommendedProduct[index] = categoryProductList[subIndex];
                    index++;
                }
            }
        }
    }

    private ProductDetail[] getSubSubCategoryProductList(CacheConnect cache, HashMap<String, String> productMap, HashMap<String, ProductDetail> recommendedProductDetail, int numberOfRecommendedProducts) {
        if (subSubCategoryId != null) {
            String productList = productMap.get(namespace + "_4_" + subSubCategoryId);
            if (productList == null) {
                productList = cache.get(namespace + "_4_" + subSubCategoryId);
                if (productList == null) {
                    productList = "-";
                }
                productMap.put(namespace + "_4_" + subSubCategoryId, productList);
            }
            if (productList.equals("-")) {
                return null;
            } else {
                return getProductDataFromList(cache, productList, recommendedProductDetail, numberOfRecommendedProducts);
            }
        } else {
            return null;
        }
    }

    private ProductDetail[] getSubCategoryProductList(CacheConnect cache, HashMap<String, String> productMap, HashMap<String, ProductDetail> recommendedProductDetail, int numberOfRecommendedProducts) {
        if (subCategoryId != null) {
            String productList = productMap.get(namespace + "_3_" + subCategoryId);
            if (productList == null) {
                productList = cache.get(namespace + "_3_" + subCategoryId);
                if (productList == null) {
                    productList = "-";
                }
                productMap.put(namespace + "_3_" + subCategoryId, productList);
            }
            if (productList.equals("-")) {
                return null;
            } else {
                return getProductDataFromList(cache, productList, recommendedProductDetail, numberOfRecommendedProducts);
            }
        } else {
            return null;
        }
    }

    private ProductDetail[] getCategoryProductList(CacheConnect cache, HashMap<String, String> productMap, HashMap<String, ProductDetail> recommendedProductDetail, int numberOfRecommendedProducts) {
        if (categoryId != null) {
            String productList = productMap.get(namespace + "_2_" + categoryId);
            if (productList == null) {
                productList = cache.get(namespace + "_2_" + categoryId);
                if (productList == null || productList.length() == 0) {
                    productList = "-";
                }
                productMap.put(namespace + "_2_" + categoryId, productList);
            }
            if (productList.equals("-")) {
                return null;
            } else {
                return getProductDataFromList(cache, productList, recommendedProductDetail, numberOfRecommendedProducts);
            }
        } else {
            return null;
        }
    }

    public void jsonToProductDetail(String productData) {
        if (productData != null) {
            try {
                JSONParser jsonParser = new JSONParser();
                JSONArray jsonArray = (JSONArray) jsonParser.parse(productData);
                Iterator iterator = jsonArray.iterator();
                int index = 0;
                while (iterator.hasNext()) {
                    switch (index) {
                        case 0:
                            setProductName((String) iterator.next());
                            break;
                        case 1:
                            setLandingPageUrl((String) iterator.next());
                            break;
                        case 2:
                            setCdnUrl((String) iterator.next());
                            break;
                        case 4:
                            categoryId = ((String) iterator.next()).replace(" ", "+");
                            setCategoryId(categoryId.length() != 0 ? categoryId : null);
                            break;
                        case 9:
                            subCategoryId = ((String) iterator.next()).replace(" ", "+");
                            setSubCategoryId(subCategoryId.length() != 0 ? subCategoryId : null);
                            break;
                        case 11:
                            subSubCategoryId = ((String) iterator.next()).replace(" ", "+");
                            setSubSubCategoryId(subSubCategoryId.length() != 0 ? subSubCategoryId : null);
                            break;
                        default:
                            iterator.next();
                            break;
                    }
                    index++;
                }
            } catch (ParseException ex) {
                Logger.getLogger(ProductDetail.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * @return the validProduct
     */
    public boolean isValidProduct() {
        return validProduct;
    }

    /**
     * @param validProduct the validProduct to set
     */
    public void setValidProduct(boolean validProduct) {
        this.validProduct = validProduct;
    }

    /**
     * @return the recommendedProduct
     */
    public ProductDetail[] getRecommendedProduct() {
        return recommendedProduct;
    }

    /**
     * @param recommendedProduct the recommendedProduct to set
     */
    public void setRecommendedProduct(ProductDetail[] recommendedProduct) {
        this.recommendedProduct = recommendedProduct;
    }

    private ProductDetail[] getProductDataFromList(CacheConnect cache, String productList, HashMap<String, ProductDetail> recommendedProductDetail, int numberOfRecommendedProducts) {
        String[] productIdArray = productList.replace("\"", "").split(",");
        List<ProductDetail> productDetailList = new ArrayList<>();
        List<ProductDetail> requestProductDetailList= new ArrayList<>();
        for (String pid : productIdArray) {
            if (!pid.equals(productId)) {
                if (!recommendedProductDetail.containsKey(namespace + "_1_" + pid)) {
                    requestProductDetailList.add(new ProductDetail(pid, namespace));
                }
                productDetailList.add(new ProductDetail(pid, namespace));
            }
        }
        Map<String, Object> productDetailMap = cache.getBulk(requestProductDetailList, "_1_");
        if (productDetailMap != null) {
            ListIterator iterator = productDetailList.listIterator();
            while (iterator.hasNext()) {
                ProductDetail productDetail = (ProductDetail) iterator.next();
                if (productDetailMap.containsKey(namespace + "_1_" + productDetail.getProductId())) {
                    productDetail.jsonToProductDetail((String) productDetailMap.get(namespace + "_1_" + productDetail.getProductId()));
                    recommendedProductDetail.put(namespace + "_1_" + productDetail.getProductId(), productDetail);
                } else {
                    iterator.set(recommendedProductDetail.get(namespace + "_1_" + productDetail.getProductId()));
                }
            }
        } else {
            return null;
        }
        if (productDetailList.size() <= numberOfRecommendedProducts) {
            return productDetailList.toArray(new ProductDetail[productDetailList.size()]);
        } else {
            Random rand = new Random();
            int randomIndex;
            int index;
            ProductDetail[] productDetail = new ProductDetail[numberOfRecommendedProducts];
            for (index = 0; index < numberOfRecommendedProducts; index++) {
                randomIndex = rand.nextInt(productDetailList.size());
                productDetail[index] = productDetailList.get(randomIndex);
                productDetailList.remove(randomIndex);
            }
            return productDetail;
        }
    }
}
