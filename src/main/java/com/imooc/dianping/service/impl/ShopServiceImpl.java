package com.imooc.dianping.service.impl;

import com.google.gson.*;
import com.imooc.dianping.common.BusinessException;
import com.imooc.dianping.common.EmBusinessError;
import com.imooc.dianping.dal.ShopModelMapper;
import com.imooc.dianping.model.CategoryModel;
import com.imooc.dianping.model.SellerModel;
import com.imooc.dianping.model.ShopModel;
import com.imooc.dianping.recommend.service.RecommendService;
import com.imooc.dianping.service.CategoryService;
import com.imooc.dianping.service.SellerService;
import com.imooc.dianping.service.ShopService;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thymeleaf.util.MapUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ShopServiceImpl implements ShopService {

    private final static String INDEX_NAME = "shop";

    @Autowired
    private ShopModelMapper shopModelMapper;
    @Autowired
    private CategoryService categoryService;
    @Autowired
    private SellerService sellerService;
    @Autowired
    private RestHighLevelClient rhlClient;
    @Autowired
    private Gson gson;
    @Autowired
    private RecommendService recommendService;

    @Override
    @Transactional
    public ShopModel create(ShopModel shopModel) throws BusinessException {
        shopModel.setCreatedAt(new Date());
        shopModel.setUpdatedAt(new Date());

        //校验商家是否存在正确
        SellerModel sellerModel = sellerService.get(shopModel.getSellerId());
        if (sellerModel == null) {
            throw new BusinessException(EmBusinessError.PARAMETER_VALIDATION_ERROR, "商户不存在");
        }

        if (sellerModel.getDisabledFlag().intValue() == 1) {
            throw new BusinessException(EmBusinessError.PARAMETER_VALIDATION_ERROR, "商户已禁用");
        }

        //校验类目
        CategoryModel categoryModel = categoryService.get(shopModel.getCategoryId());
        if (categoryModel == null) {
            throw new BusinessException(EmBusinessError.PARAMETER_VALIDATION_ERROR, "类目不存在");
        }
        shopModelMapper.insertSelective(shopModel);

        return get(shopModel.getId());
    }

    @Override
    public ShopModel get(Integer id) {
        ShopModel shopModel = shopModelMapper.selectByPrimaryKey(id);
        if (shopModel == null) {
            return null;
        }
        shopModel.setSellerModel(sellerService.get(shopModel.getSellerId()));
        shopModel.setCategoryModel(categoryService.get(shopModel.getCategoryId()));
        return shopModel;
    }

    @Override
    public List<ShopModel> selectAll() {
        List<ShopModel> shopModelList = shopModelMapper.selectAll();
        shopModelList.forEach(shopModel -> {
            shopModel.setSellerModel(sellerService.get(shopModel.getSellerId()));
            shopModel.setCategoryModel(categoryService.get(shopModel.getCategoryId()));
        });
        return shopModelList;
    }

    @Override
    public List<ShopModel> recommend(BigDecimal longitude, BigDecimal latitude) {
        // ASL算法召回并使用LR算法排序
        List<Integer> shopIdList = recommendService.recall(148);//TODO 此处传的应该是登录用户的id
        shopIdList = recommendService.sort(shopIdList,148);//TODO 此处传的应该是登录用户的id
        List<ShopModel> shopModelList = shopIdList.stream()
                .map(this::get)
                .collect(Collectors.toList());

        // V1.0，根据用户地理位置按MySQL计算公式推荐
//        List<ShopModel> shopModelList = shopModelMapper.recommend(longitude, latitude);
//        shopModelList.forEach(shopModel -> {
//            shopModel.setSellerModel(sellerService.get(shopModel.getSellerId()));
//            shopModel.setCategoryModel(categoryService.get(shopModel.getCategoryId()));
//        });
        return shopModelList;
    }

    @Override
    public List<Map<String, Object>> searchGroupByTags(String keyword, Integer categoryId, String tags) {
        return shopModelMapper.searchGroupByTags(keyword, categoryId, tags);
    }

    @Override
    public Integer countAllShop() {
        return shopModelMapper.countAllShop();
    }

    @Override
    public List<ShopModel> search(BigDecimal longitude,
                                  BigDecimal latitude, String keyword, Integer orderby,
                                  Integer categoryId, String tags) {
        List<ShopModel> shopModelList = shopModelMapper.search(longitude, latitude, keyword, orderby, categoryId, tags);
        shopModelList.forEach(shopModel -> {
            shopModel.setSellerModel(sellerService.get(shopModel.getSellerId()));
            shopModel.setCategoryModel(categoryService.get(shopModel.getCategoryId()));
        });
        return shopModelList;
    }

    @Override
    public Map<String, Object> searchEs(BigDecimal longitude, BigDecimal latitude, String keyword, Integer orderby, Integer categoryId, String tags) {
        Map<String, Object> resultMap = new HashMap<>();
        List<ShopModel> list = new ArrayList<>();
        List<Map<String, Object>> tagBuckets = new ArrayList<>();
        // 使用高级Client完成简单查询
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
//                .query(QueryBuilders.nameQuery("name", keyword))
//                .timeout(new TimeValue(60, TimeUnit.SECONDS));
//        SearchRequest searchReq = new SearchRequest(INDEX_NAME)
//                .source(sourceBuilder);
//        try {
//            final SearchResponse response = rhlClient.search(searchReq, RequestOptions.DEFAULT);
//            list = Arrays.stream(response.getHits().getHits())
//                    .map(doc -> {
//                        final int id = Integer.parseInt(doc.getSourceAsMap().get("id").toString());
//                        return this.get(id);
//                    }).collect(Collectors.toList());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        // 使用低级Client实现复杂查询
//        String reqJson = "{\n" +
//                "  \"_source\": \"*\",\n" +
//                "  \"script_fields\": {\n" +
//                "    \"distance\": {\n" +
//                "      \"script\": {\n" +
//                "        \"source\": \"haversin(lat,lon,doc['location'].lat,doc['location'].lon)\",\n" +
//                "        \"lang\": \"expression\",\n" +
//                "        \"params\": {\n" +
//                "          \"lat\": " + latitude + ",\n" +
//                "          \"lon\": " + longitude + "\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"query\": {\n" +
//                "    \"function_score\": {\n" +
//                "      \"query\": {\n" +
//                "        \"bool\": {\n" +
//                "          \"must\": [\n" +
//                "            {\n" +
//                "              \"match\": {\n" +
//                "                \"name\": {\n" +
//                "                  \"query\": \"" + keyword + "\",\n" +
//                "                  \"boost\":0.1\n" +
//                "                }\n" +
//                "              }\n" +
//                "            },\n" +
//                "            {\n" +
//                "              \"term\": {\n" +
//                "                \"seller_disabled_flag\": { \n" +
//                "                  \"value\": 0\n" +
//                "                }\n" +
//                "              }\n" +
//                "            }\n" +
//                "          ]\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"functions\": [\n" +
//                "        {\n" +
//                "          \"gauss\": {\n" +
//                "            \"location\": {\n" +
//                "              \"origin\": \"" + latitude + "," + longitude + "\",\n" +
//                "              \"scale\": \"100km\", \n" +
//                "              \"offset\": \"0km\", \n" +
//                "              \"decay\": 0.5\n" +
//                "            }\n" +
//                "          },\n" +
//                "          \"weight\": 9   \n" +
//                "        },\n" +
//                "        {\n" +
//                "          \"field_value_factor\": {\n" +
//                "            \"field\": \"remark_score\"\n" +
//                "          },\n" +
//                "          \"weight\": 0.2  \n" +
//                "        },\n" +
//                "        {\n" +
//                "          \"field_value_factor\": {\n" +
//                "            \"field\": \"seller_remark_score\"\n" +
//                "          },\n" +
//                "          \"weight\": 0.1  \n" +
//                "        }\n" +
//                "      ],\n" +
//                "      \"score_mode\": \"sum\",  \n" +
//                "      \"boost_mode\": \"sum\"   \n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"sort\": [\n" +
//                "    {\n" +
//                "      \"_score\": {\n" +
//                "        \"order\": \"desc\"\n" +
//                "      }\n" +
//                "    }\n" +
//                "  ]\n" +
//                "}";
        // 面向对象的方法构造Json
        JsonObject reqJsonObj = new JsonObject();
        // source字段
        reqJsonObj.addProperty("_source", "*");
        // 自定义脚本距离字段
        reqJsonObj.add("script_fields", new JsonObject());
        reqJsonObj.getAsJsonObject("script_fields").add("distance", new JsonObject());
        reqJsonObj.getAsJsonObject("script_fields").getAsJsonObject("distance").add("script", new JsonObject());
        reqJsonObj.getAsJsonObject("script_fields").getAsJsonObject("distance").getAsJsonObject("script")
                .addProperty("source", "haversin(lat,lon,doc['location'].lat,doc['location'].lon)");
        reqJsonObj.getAsJsonObject("script_fields").getAsJsonObject("distance").getAsJsonObject("script")
                .addProperty("lang", "expression");
        reqJsonObj.getAsJsonObject("script_fields").getAsJsonObject("distance").getAsJsonObject("script")
                .add("params", new JsonObject());
        reqJsonObj.getAsJsonObject("script_fields").getAsJsonObject("distance").getAsJsonObject("script")
                .getAsJsonObject("params").addProperty("lat", latitude);
        reqJsonObj.getAsJsonObject("script_fields").getAsJsonObject("distance").getAsJsonObject("script")
                .getAsJsonObject("params").addProperty("lon", longitude);
        // function_score字段
        reqJsonObj.add("query", new JsonObject());
        reqJsonObj.getAsJsonObject("query").add("function_score", new JsonObject());
        // function_score中的query字段
        reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score").add("query", new JsonObject());
        reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score").getAsJsonObject("query")
                .add("bool", new JsonObject());
        // 构造must召回匹配条件
        final JsonArray mustArray = new JsonArray();
        reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score").getAsJsonObject("query")
                .getAsJsonObject("bool").add("must", mustArray);
        final JsonObject name = new JsonObject();
        name.add("match", new JsonObject());
        name.getAsJsonObject("match").add("name", new JsonObject());
        name.getAsJsonObject("match").getAsJsonObject("name").addProperty("query", keyword);
        name.getAsJsonObject("match").getAsJsonObject("name").addProperty("boost", 0.1);
        final JsonObject isDisabled = new JsonObject();
        isDisabled.add("term", new JsonObject());
        isDisabled.getAsJsonObject("term").addProperty("seller_disabled_flag", 0);
        mustArray.add(isDisabled);
        if (categoryId != null) {  //若用户选中了类目
            final JsonObject category = new JsonObject();
            category.add("term", new JsonObject());
            category.getAsJsonObject("term").addProperty("category_id", categoryId);
            mustArray.add(category);
        }
        if (tags != null) {  //若用户选中了标签
            final JsonObject tag = new JsonObject();
            tag.add("term", new JsonObject());
            tag.getAsJsonObject("term").addProperty("tags", tags);
            mustArray.add(tag);
        }

        final Map<String, Integer> categoryByKeyWordMap = this.analyzeCategoryByKeyWord(keyword);
        // 相关性不能既影响召回又影响排序，一起使用会一起加分，导致召回的文档排序混乱不符合需求。
        // 比如召回打分高达0.9但排序打分只有0.1，那再把两个评分相加就会破坏打分标杆。
        // 最佳实践：一般我们不会选择作用于召回规则（防止语义理解错误导致过多无关的结果），而是使用排序策略来提高搜索结果的相关性
        boolean isAffectFilter = true; //相关性应用于召回策略的开关(使用should)
        boolean isAffectOrder = false;   //相关性应用于排序策略的开关

        if (isAffectFilter && !MapUtils.isEmpty(categoryByKeyWordMap)) {  //若影响召回打分，使用should
            final JsonObject bool = new JsonObject();
            mustArray.add(bool);
            final JsonArray shouldArray = new JsonArray();
            bool.add("bool", new JsonObject());
            bool.getAsJsonObject("bool").add("should", shouldArray);
            shouldArray.add(name);
            // 把所有搜索分词相关的类目id全部增加到should条件中，负责文档召回
            categoryByKeyWordMap.forEach((keywordToken, relativeCategoryId) -> {
                final JsonObject category = new JsonObject();
                category.add("term", new JsonObject());
                category.getAsJsonObject("term").add("category_id", new JsonObject());
                category.getAsJsonObject("term").getAsJsonObject("category_id")
                        .addProperty("value", relativeCategoryId);
                // 我们要求此处的category条件只负责召回，不影响打分（解耦），而在下边的function_score中再通过filter对这些category进行打分
                category.getAsJsonObject("term").getAsJsonObject("category_id")
                        .addProperty("boost", 0);
                shouldArray.add(category);
            });
        } else { //若相关性不应用于召回，使用must
            mustArray.add(name);
        }
        // 构造functions
        final JsonArray functionsArray = new JsonArray();
        reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score")
                .add("functions", functionsArray);
        if (orderby == null) {  //当前端没有指定排序规则时的默认排序规则
            final JsonObject gauss = new JsonObject();
            gauss.add("gauss", new JsonObject());
            gauss.getAsJsonObject("gauss").add("location", new JsonObject());
            gauss.getAsJsonObject("gauss").getAsJsonObject("location")
                    .addProperty("origin", latitude + "," + longitude);
            gauss.getAsJsonObject("gauss").getAsJsonObject("location")
                    .addProperty("scale", "100km");
            gauss.getAsJsonObject("gauss").getAsJsonObject("location")
                    .addProperty("offset", "0km");
            gauss.getAsJsonObject("gauss").getAsJsonObject("location")
                    .addProperty("decay", 0.5);
            gauss.addProperty("weight", 9);
            functionsArray.add(gauss);
            final JsonObject remarkScoreFactor = new JsonObject();
            remarkScoreFactor.add("field_value_factor", new JsonObject());
            remarkScoreFactor.getAsJsonObject("field_value_factor").addProperty("field", "remark_score");
            remarkScoreFactor.addProperty("weight", 0.2);
            functionsArray.add(remarkScoreFactor);
            final JsonObject sellerRemarkScoreFactor = new JsonObject();
            sellerRemarkScoreFactor.add("field_value_factor", new JsonObject());
            sellerRemarkScoreFactor.getAsJsonObject("field_value_factor").addProperty("field", "seller_remark_score");
            sellerRemarkScoreFactor.addProperty("weight", 0.1);
            functionsArray.add(sellerRemarkScoreFactor);
            // 对每个搜索分词相关的类目id都使用filter进行打分
            if (isAffectOrder && !MapUtils.isEmpty(categoryByKeyWordMap)) {
                categoryByKeyWordMap.forEach((keywordToken, relativeCategoryId) -> {
                    final JsonObject filter = new JsonObject();
                    final JsonObject categoryTerm = new JsonObject();
                    categoryTerm.add("term", new JsonObject());
                    categoryTerm.getAsJsonObject("term").add("category_id", new JsonObject());
                    categoryTerm.getAsJsonObject("term").getAsJsonObject("category_id")
                            .addProperty("value", relativeCategoryId);
                    filter.add("filter", categoryTerm);
                    filter.addProperty("weight", 3);  //适当调大权重(数据调试),从而使 相关性 优先于 地理位置和用户/商家评分
                    functionsArray.add(filter);
                });
            }
            // 指定分数计算模式
            reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score")
                    .addProperty("score_mode", "sum");
            reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score")
                    .addProperty("boost_mode", "sum");
        } else {  //非默认排序，即低价排序
            final JsonObject pricePerMan = new JsonObject();
            pricePerMan.add("field_value_factor", new JsonObject());
            pricePerMan.getAsJsonObject("field_value_factor").addProperty("field", "price_per_man");
            pricePerMan.addProperty("weight", 1);
            functionsArray.add(pricePerMan);
            // 指定分数计算模式
            reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score")
                    .addProperty("score_mode", "sum");
            reqJsonObj.getAsJsonObject("query").getAsJsonObject("function_score")
                    .addProperty("boost_mode", "replace");  //屏蔽functions的query对打分的影响
        }
        // 排序字段
        final JsonArray sortArray = new JsonArray();
        reqJsonObj.add("sort", sortArray);
        final JsonObject score = new JsonObject();
        score.add("_score", new JsonObject());
        if (orderby == null) {    //当前端没有指定排序规则时的默认排序规则
            score.getAsJsonObject("_score").addProperty("order", "desc");
        } else {  //非默认排序，即低价排序
            score.getAsJsonObject("_score").addProperty("order", "asc");
        }
        sortArray.add(score);
        // 聚合标签字段，返回给各个标签给用户进一步筛选
        reqJsonObj.add("aggs", new JsonObject());
        reqJsonObj.getAsJsonObject("aggs").add("group_by_tags", new JsonObject());
        reqJsonObj.getAsJsonObject("aggs").getAsJsonObject("group_by_tags").add("terms", new JsonObject());
        reqJsonObj.getAsJsonObject("aggs").getAsJsonObject("group_by_tags").getAsJsonObject("terms")
                .addProperty("field", "tags");

        final String reqJson = reqJsonObj.toString();
        System.out.println(reqJson);
        Request request = new Request(HttpGet.METHOD_NAME, "/" + INDEX_NAME + "/_search");
        request.setJsonEntity(reqJson);
        try {
            Response response = rhlClient.getLowLevelClient().performRequest(request);
            JsonObject jsonObject = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
            JsonArray hits = jsonObject.getAsJsonObject("hits").getAsJsonArray("hits");
            for (JsonElement doc : hits) {
                JsonObject docObj = doc.getAsJsonObject();
                Integer id = docObj.get("_id").getAsInt();
                BigDecimal distanceKm = new BigDecimal(docObj.getAsJsonObject("fields").getAsJsonArray("distance").get(0).toString());
                final ShopModel shopModel = this.get(id);
                // km -> m 然后取整
                int distanceM = distanceKm.multiply(BigDecimal.valueOf(1000).setScale(0, BigDecimal.ROUND_CEILING)).intValue();
                shopModel.setDistance(distanceM);
                list.add(shopModel);
            }
            //取出聚合字段
            final JsonArray bucketsJsonArray = jsonObject.getAsJsonObject("aggregations").getAsJsonObject("group_by_tags").getAsJsonArray("buckets");
            for (JsonElement bucket : bucketsJsonArray) {
                Map<String, Object> map = new HashMap<>();
                map.put("tags", bucket.getAsJsonObject().get("key").getAsString());
                map.put("num", bucket.getAsJsonObject().get("doc_count").getAsInt());
                tagBuckets.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        resultMap.put("shop", list);
        resultMap.put("tags", tagBuckets);
        return resultMap;
    }


    //人工标注得到的categoryId与相关搜索词的映射
    private Map<Integer, List<String>> categoryId2WordMap = new HashMap<>();

    @PostConstruct
    public void iniCategoryId2WordMap() {
        final List<String> foods = new ArrayList<>();
        foods.add("吃饭");
        foods.add("下午茶");
        foods.add("美食");
        final List<String> hotels = new ArrayList<>();
        hotels.add("休息");
        hotels.add("睡觉");
        hotels.add("住宿");
        categoryId2WordMap.put(1, foods);
        categoryId2WordMap.put(2, hotels);
    }

    /**
     * 根据词汇匹配类目id
     *
     * @param token 搜索词的一个分词
     */
    private Integer getCategoryIdByToken(String token) {
        for (Map.Entry<Integer, List<String>> categoryId2Word : categoryId2WordMap.entrySet()) {
            if (categoryId2Word.getValue().contains(token)) {
                return categoryId2Word.getKey();
            }
        }
        return null;
    }

    /**
     * 根据用户搜索词，找到相关的类目id
     * 具体实现：先分析用户输入的搜索词得到每个词条，然后去人工标注得到的 categoryId与相关搜索词的映射 中找到它们对应的categoryId
     *
     * @return Map<用户搜索词分词token, categoryId>
     */
    private Map<String, Integer> analyzeCategoryByKeyWord(String keyword) {
        Map<String, Integer> resMap = new HashMap<>();
        //用shop索引的name的分析器(已配置ik自定义词库和同义词词库)去得到分词结果
        AnalyzeRequest analyzeReq = AnalyzeRequest
                .withField(INDEX_NAME, "name", keyword);
        System.out.println(gson.toJson(analyzeReq));
        try {
            final AnalyzeResponse response = rhlClient.indices().analyze(analyzeReq, RequestOptions.DEFAULT);
            // 获取词条分析结果 The token is the actual term that will be stored in the index
            List<AnalyzeResponse.AnalyzeToken> analyzeTokenList = response.getTokens();
            for (AnalyzeResponse.AnalyzeToken token : analyzeTokenList) {
                String tokenTerm = token.getTerm();
                Integer categoryId = this.getCategoryIdByToken(tokenTerm);
                if (categoryId != null) {
                    resMap.put(tokenTerm, categoryId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resMap;
    }

}
