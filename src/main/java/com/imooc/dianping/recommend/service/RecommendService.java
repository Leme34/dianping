package com.imooc.dianping.recommend.service;

import com.imooc.dianping.dal.RecommendDOMapper;
import com.imooc.dianping.model.RecommendDO;
import com.imooc.dianping.model.ShopSortModel;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 查询离线在Spark平台使用ALS算法（粗排）推荐的商铺
 * <p>
 * Created by lsd
 * 2020-03-22 20:42
 */
@Service
public class RecommendService {

    @Autowired
    private RecommendDOMapper recommendDOMapper;
    @Autowired
    private ApplicationContext applicationContext;
    private SparkSession spark;
    private LogisticRegressionModel lrModel;

    @PostConstruct
    public void init() {
        spark = applicationContext.getBean(SparkSession.class);
        //加载训练好的LR模型
        lrModel = LogisticRegressionModel.load("file:///C:/Users/Administrator/Desktop/lrModel");
    }

    /**
     * ALS算法召回（推荐）商铺
     *
     * @param userId 推荐目标用户
     * @return 推荐商铺id列表
     */
    public List<Integer> recall(Integer userId) {
        RecommendDO recommendDO = recommendDOMapper.selectByPrimaryKey(userId);
        // 若没有该用户的离线推荐
        if (recommendDO == null) {
            recommendDO = recommendDOMapper.selectByPrimaryKey(9999999); //这条数据是默认推荐
        }
        String[] shopIdArr = recommendDO.getShopIds().split(",");
        List<Integer> shopIdList = new ArrayList<>();
        for (String s : shopIdArr) {
            shopIdList.add(Integer.valueOf(s));
        }
        return shopIdList;
    }

    /**
     * LR算法实现推荐数据的排序
     *
     * @param shopIdList （粗排后）待精排的推荐商铺
     * @param userId     推荐目标用户
     * @return 推荐商铺id列表
     */
    public List<Integer> sort(List<Integer> shopIdList, Integer userId) {
        //需要根据LR模型所需要11维的特征值x，做特征处理，然后调用其预测方法
        List<ShopSortModel> list = new ArrayList<>();
        for (Integer shopId : shopIdList) {
            //造的假数据，可以从数据库或缓存中拿到对应的性别，年龄，评分，价格等做特征处理生成特征向量
            Vector v = Vectors.dense(1, 0, 0, 0, 0, 1, 0.6, 0, 0, 1, 0);
//            Vector result = lrModel.predict(v);           //若偏向正样本则1，否则偏向负样本为0
            Vector result = lrModel.predictProbability(v);  //获取 结果是正样本还是负样本的概率 的预测值
            double[] arr = result.toArray();
//            double score = arr[0]; //负样本概率
            double score = arr[1];   //正样本概率
            ShopSortModel shopSortModel = new ShopSortModel().setShopId(shopId).setScore(score);
            list.add(shopSortModel);
        }
        // 按照正样本概率倒序排序
        list.sort((o1, o2) -> Double.compare(o2.getScore(), o1.getScore()));
        return list.stream().map(ShopSortModel::getShopId).collect(Collectors.toList());
    }


}
