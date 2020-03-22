package com.imooc.dianping.controller;

import com.imooc.dianping.common.BusinessException;
import com.imooc.dianping.common.CommonRes;
import com.imooc.dianping.common.EmBusinessError;
import com.imooc.dianping.model.CategoryModel;
import com.imooc.dianping.model.ShopModel;
import com.imooc.dianping.service.CategoryService;
import com.imooc.dianping.service.ShopService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller("/shop")
@RequestMapping("/shop")
public class ShopController {

    @Autowired
    private ShopService shopService;

    @Autowired
    private CategoryService categoryService;

    /**
     * 推荐服务
     */
    @RequestMapping("/recommend")
    @ResponseBody
    public CommonRes recommend(@RequestParam(name = "longitude") BigDecimal longitude,
                               @RequestParam(name = "latitude") BigDecimal latitude) throws BusinessException {
        if (longitude == null || latitude == null) {
            throw new BusinessException(EmBusinessError.PARAMETER_VALIDATION_ERROR);
        }

        List<ShopModel> shopModelList = shopService.recommend(longitude, latitude);
        return CommonRes.create(shopModelList);
    }


    //搜索服务V1.0
    @RequestMapping("/search")
    @ResponseBody
    public CommonRes search(@RequestParam(name = "longitude") BigDecimal longitude,
                            @RequestParam(name = "latitude") BigDecimal latitude,
                            @RequestParam(name = "keyword") String keyword,
                            @RequestParam(name = "orderby", required = false) Integer orderby,
                            @RequestParam(name = "categoryId", required = false) Integer categoryId,
                            @RequestParam(name = "tags", required = false) String tags) throws BusinessException {
        if (StringUtils.isEmpty(keyword) || longitude == null || latitude == null) {
            throw new BusinessException(EmBusinessError.PARAMETER_VALIDATION_ERROR);
        }

        //List<ShopModel> shopModelList = shopService.search(longitude,latitude,keyword,orderby,categoryId,tags);
//        List<Map<String,Object>> tagsAggregation = shopService.searchGroupByTags(keyword,categoryId,tags);
        final Map<String, Object> resultMap = shopService.searchEs(longitude, latitude, keyword, orderby, categoryId, tags);
        List<ShopModel> shopModelList = (List<ShopModel>) resultMap.get("shop");
        List<Map<String, Object>> tagsAggregation = (List<Map<String, Object>>) resultMap.get("tags");

        List<CategoryModel> categoryModelList = categoryService.selectAll();
        Map<String, Object> resMap = new HashMap<>();
        resMap.put("shop", shopModelList);
        resMap.put("category", categoryModelList);
        resMap.put("tags", tagsAggregation);
        return CommonRes.create(resMap);

    }


}
