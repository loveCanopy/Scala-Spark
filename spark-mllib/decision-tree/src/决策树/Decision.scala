package 决策树

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.linalg.Vectors

object Decision {
  
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("Decision")
    val sc=new SparkContext(conf)
    val rawData=sc.textFile("E:\\mllib\\CovType\\covtype.data\\covtype.data", 1)
    val data=rawData.map { line => 
        val values=line.split(",").map { _.toDouble } 
        val featureVector=Vectors.dense(values.init)
        val label=values.last-1
        LabeledPoint(label,featureVector)
   
    }
//     data.take(5).foreach { println }
//     (4.0,[2596.0,51.0,3.0,258.0,0.0,510.0,221.0,232.0,148.0,6279.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0])
//(4.0,[2590.0,56.0,2.0,212.0,-6.0,390.0,220.0,235.0,151.0,6225.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0])
//(1.0,[2804.0,139.0,9.0,268.0,65.0,3180.0,234.0,238.0,135.0,6121.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0])
//(1.0,[2785.0,155.0,18.0,242.0,118.0,3090.0,238.0,238.0,122.0,6211.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0])
//(4.0,[2595.0,45.0,2.0,153.0,-1.0,391.0,220.0,234.0,150.0,6172.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0])
//     
    //将数据分为完整的三部分 训练集80%，交叉检验集10%，测试集10%
    val Array(trainData,cvData,testData)=data.randomSplit(Array(0.8,0.1,0.1))
    //缓存数据
    trainData.cache()
    cvData.cache()
    testData.cache()
    
    //在训练集上构造分类树模型
    val model=DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "gini", 4, 100)
    
    def getMetrics(model:DecisionTreeModel,data:RDD[LabeledPoint]):MulticlassMetrics={
      val predictionAndLabels=data.map { example => (model.predict(example.features),example.label) }
      new MulticlassMetrics(predictionAndLabels)
      
    }
    //用CVdata预测
    val metrics=getMetrics(model,cvData)
//    println(metrics.precision) //输出准确率
    //0 到 6 共七类 打印每一类的精确度，召回率
    (0 until 7).map(cat =>(metrics.precision(cat),metrics.recall(cat))).foreach(println)
    
    //对准确度评估
    def classPro(data:RDD[LabeledPoint]):Array[Double]={
      
      val countByKey=data.map(_.label).countByValue() //(类别，数量)
      val counts=countByKey.toArray.sortBy(_._1).map(_._2)
      
      counts.map { _.toDouble/counts.sum }
      
      
    }
    val TrainPro=classPro(trainData)
    val CvPro=classPro(cvData)
    val numPreci=TrainPro.zip(CvPro).map{case(train,cv)=>train*cv}.sum
    
    //决策树调优
    
    //随机森林
    val forest=RandomForest.trainClassifier(trainData, 7, Map(10 -> 4,11 -> 40), 20, "auto", "entropy", 30, 300)
//     def getMetrics_forest(model:RandomForestModel,data:RDD[LabeledPoint]):MulticlassMetrics={
//      val predictionAndLabels=data.map { example => (model.predict(example.features),example.label) }
//      new MulticlassMetrics(predictionAndLabels)
//      
//    }
//     getMetrics_forest(forest.trees,cvData)
//       var pre_forest=0.0
//    for( tree <- forest.trees){
//       var metrics=getMetrics(tree, cvData).precision
//       pre_forest=pre_forest+metrics
//    }
//   println(pre_forest/2)
//   println("___________")
    //预测
    val input="2596,51,3,258,0,510,221,232,148,6279,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0"
    val vector=Vectors.dense(input.split(',').map( _.toDouble))
    val forst_pre=forest.predict(vector)
    val model_pre=model.predict(vector)
    println(forst_pre+" "+model_pre)
    
  }
  
  
}