package com.imooc.dianping.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * shop推荐排序结构
 */
@Accessors(chain = true)
@Data
public class ShopSortModel {
    private Integer shopId;
    private double score;
}
