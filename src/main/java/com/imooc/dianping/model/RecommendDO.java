package com.imooc.dianping.model;

public class RecommendDO {
    /**
     *
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column recommend.id
     *
     * @mbg.generated Sun Sep 15 15:24:33 CST 2019
     */
    private Integer userId;

    /**
     *
     * This field was generated by MyBatis Generator.
     * This field corresponds to the database column recommend.recommend
     *
     * @mbg.generated Sun Sep 15 15:24:33 CST 2019
     */
    private String shopIds;

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column recommend.id
     *
     * @return the value of recommend.id
     *
     * @mbg.generated Sun Sep 15 15:24:33 CST 2019
     */
    public Integer getUserId() {
        return userId;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column recommend.id
     *
     * @param userId the value for recommend.id
     *
     * @mbg.generated Sun Sep 15 15:24:33 CST 2019
     */
    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method returns the value of the database column recommend.recommend
     *
     * @return the value of recommend.recommend
     *
     * @mbg.generated Sun Sep 15 15:24:33 CST 2019
     */
    public String getShopIds() {
        return shopIds;
    }

    /**
     * This method was generated by MyBatis Generator.
     * This method sets the value of the database column recommend.recommend
     *
     * @param shopIds the value for recommend.recommend
     *
     * @mbg.generated Sun Sep 15 15:24:33 CST 2019
     */
    public void setShopIds(String shopIds) {
        this.shopIds = shopIds == null ? null : shopIds.trim();
    }
}
