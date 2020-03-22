package com.imooc.dianping.recommend;

import com.google.common.collect.Maps;
import com.imooc.dianping.recommend.dto.RatingMatrix;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 个性化召回算法之ALS算法
 * 基于已有用户行为所得的用户-商铺打分矩阵，根据 用户特征矩阵 和 商铺特征矩阵 预测计算空缺的值
 * <p>
 * Created by lsd
 * 2020-03-21 15:50
 */
@RequestMapping("/als")
@RestController
public class AlsRecallController {

    //@Autowired
    private static SparkSession spark;


    /**
     * ALS召回算法模型训练
     * 1.过拟合:训练结果过分趋近于真实数据,一旦真实数据出现误差则预测结果就不尽人意。
     *   解决办法:增大数据规模;减小Rank(减少特征维度,更加松散);增大正则化系数
     * 2. 欠拟合:训练结果与真实数据偏差过大,没有很好的与真实数据收敛
     *   解决办法:增加Rank;减小正则化系数(缩小正则距离(偏差值));
     */
    //@GetMapping("/alsRecallTrain")
    public static String alsRecallTrain() {
        Dataset<Row> dataSet = loadBehaviorDataAsDataFrame();
        // 将所有数据分出80%用于训练，20%用于测试
        Dataset<Row>[] dataSets = dataSet.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingData = dataSets[0];
        Dataset<Row> testingData = dataSets[1];
        // ALS模型定义
        ALS als = new ALS()
                .setMaxIter(10)    //最大迭代次数,为了保证性能避免模型过分拟合此处设置10
                .setRank(5)        //矩阵的特征(feature)数量
                .setRegParam(0.01) //正则化系数,用于防止过拟合情况
                .setUserCol("userId") //矩阵结构字段
                .setItemCol("shopId")
                .setRatingCol("rating");
        try {
            // 模型训练并把结果保存在文件中
            ALSModel alsModel = als.fit(trainingData);
            alsModel.save("file:///C:/Users/Administrator/Desktop/alsModel");
            // 使用模型对测试数据预测"rating"值，并保存在内存表的"prediction"字段中
            Dataset<Row> predictions = alsModel.transform(testingData);
            // 计算回归模型的评估指标，用于模型参数调优
            // 求均方根误差(rmse)，({预测值与真实值的差值}的平方÷{观测次数})再开平方，因此均方根误差越小越理想
            RegressionEvaluator evaluator = new RegressionEvaluator()
                    .setMetricName("rmse")            //评测指标名称
                    .setLabelCol("rating")            //真实值所在列
                    .setPredictionCol("prediction");  //预测值所在列
            double rmse = evaluator.evaluate(predictions);
            System.out.println("rmse=" + rmse);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }
        return "success";
    }

    /**
     * 加载基于已有用户行为所得的用户-商铺打分矩阵.csv文件
     *
     * @return spark内存表
     */
    private static Dataset<Row> loadBehaviorDataAsDataFrame() {
        JavaRDD<String> csvFileRDD = spark.read().textFile("file:///E:/SpringBoot项目实战/ElasticSearch+Spark 构建高匹配度搜索服务+千人千面推荐系统/dianpingjava-master/dianpingjava/第五和第六章-基础服务之品类，门店能力建设加搜索推荐/src/main/resources/training_data/behavior.csv").toJavaRDD();
        // csv数据行 转为 RatingMatrix矩阵结构
        JavaRDD<RatingMatrix> ratingMatrixRDD = csvFileRDD.map(new Function<String, RatingMatrix>() {
            @Override
            public RatingMatrix call(String str) throws Exception {
                return RatingMatrix.parseLine(str);
            }
        });
        // RatingMatrix结构内存表
        return spark.createDataFrame(ratingMatrixRDD, RatingMatrix.class);
    }


    /**
     * 使用训练好的 ALS召回算法模型 进行召回预测（粗排）
     */
    //@GetMapping("/alsRecallTrain")
    public static String alsRecallPredict() {
        // 加载训练好的模型
        ALSModel alsModel = ALSModel.load("file:///C:/Users/Administrator/Desktop/alsModel");
        // 加载数据文件
        Dataset<Row> dataFrame = loadBehaviorDataAsDataFrame();
        // 给n个用户做离线召回预测
        int n = 5;
        Dataset<Row> users = dataFrame.select(alsModel.getUserCol()).distinct().limit(n);
        // 以下API在spark-mllib依赖2.2.0以上才有
//        alsModel.recommendForAllUsers(numItems)//给训练数据中所有的user推荐numItems个商铺
        Dataset<Row> recommendations = alsModel.recommendForUserSubset(users, 20);//给指定的user推荐numItems个商铺

        // 若使用foreach()是召回商铺持久化时每个都要开一个数据库连接,因此切分到每个分片批量持久化效率更高
        recommendations.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                // 此分片是分布式环境的，因此不能直接使用应用中Bean的数据库连接，而是要对此分片开一个数据库连接
                Connection connection = DriverManager.getConnection(
                        "jdbc:mysql://127.0.0.1:3606/dianpingdb?useUnicode=true&characterEncoding=UTF-8",
                        "root", "123456");
                PreparedStatement pstmt = connection.prepareStatement(
                        "insert into recommend(user_id,shop_ids)values (?,?)"
                );
                // 遍历此分片分配到的数据Row
                List<Map<String, Object>> rowDataMaps = new ArrayList<>();
                rowIterator.forEachRemaining(action -> {
                    int userId = action.getInt(0);
                    // 已经按照打分排序了的列表
                    List<GenericRowWithSchema> recommendations = action.getList(1);
                    List<Integer> shopIds = recommendations.stream()
                            .map(row -> {
                                int shopId = row.getInt(0); //商铺id
                                float score = row.getFloat(1);  //打分
                                return shopId;
                            }).collect(Collectors.toList());
                    String shopIdsStr = StringUtils.join(shopIds, ",");
                    Map<String, Object> userId2ShopIdMap = Maps.newHashMapWithExpectedSize(1);
                    userId2ShopIdMap.put("userId", userId);
                    userId2ShopIdMap.put("shopIdsStr", shopIdsStr);
                    rowDataMaps.add(userId2ShopIdMap);
                });
                for (Map<String, Object> rowDataMap : rowDataMaps) {
                    pstmt.setInt(1, (Integer) rowDataMap.get("userId"));
                    pstmt.setString(2, (String) rowDataMap.get("shopIdsStr"));
                    pstmt.addBatch();  //拼装到批量sql中
                }
                pstmt.executeBatch();  //执行批量sql
                connection.close();
            }
        });
        return "success";
    }


    public static void main(String[] args) {
        // 必须设置此环境变量且必须是配置好winutils.exe的hadoop，否则NPE
        System.setProperty("hadoop.home.dir", "D:/解决winutils.exe问题的Hadoop/hadoop-2.6.0/");
        spark = SparkSession.builder().master("local").appName("dianping").getOrCreate();
//        alsRecallTrain();
        alsRecallPredict();
    }

}
