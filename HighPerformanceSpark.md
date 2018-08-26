###[è¦ç´/ç¿»è¨³] High Performance Spark
 
ä»ã®æ¸ç±ãæå ±ã§ã¯èªããã¦ããªããã¨ãä¸­å¿ã«åå¿é²ã¨ãã¦  
(åè) â ç§ã®ä»ãè¶³ã

##æ¸ç±ã
###æå ±
2017åºç  
Spark version 2.0.1ä»¥éã

[OReilly High Perfomance Spark](https://www.amazon.co.jp/High-Performance-Spark-Practices-Optimizing/dp/1491943203)    

  
###ç¹å¾´
Pure RDDãä¸­å¿ã«Sparkã®åé¨ã®åãã®çè§£ãæ·±ãã¦ããæµãã  
èª­èå±¤ã¯ä¸­ç´èä»¥ä¸ãæ³å®ãã¦ããã¿ããã ããåç´èã®æ¹ããããèª­ãã æ¹ãè¯ãåå®¹ããã
ããã©ã¼ãã³ã¹ã®ããã«ããã¯ã¨ãªãè¦ç´ ã«çµã£ã¦è¦ç¹ãèª¬æã
DataFrameãDatasetã®RDDã¨ã®éããå©ç¹æ¬ ç¹ãè¿°ã¹ã¦ããã

##ç¬¬ï¼ç«  
###Why Scala
æ¬æ¸ã§ã¯SparkApiãä½¿ãè¨èªã¨ãã¦Scalaãé¸å®ãã¦ããã  
ã¾ããSparkã®ããã©ã¼ãã³ã¹ã«ãã ãããããªããScalaãä½¿ç¨ãããã¨ãå¼·ãæ¨å¥¨ãã¦ããã  
çç±ã¯ä»¥ä¸  
ã»Sparkãscalaã§æ¸ããã¦ãããã¨ã  
ã»Sparkãscalaã®collectionsApiãã¨ã¦ãããçä¼¼ã¦ãããã¨ãããè¨æ³ãç´æçã§ã³ã¼ãã£ã³ã°ãå®¹æ(Java7ã¨ã®å·®å¥å)  
ã»REPLsããããã¨ã(Javaã«ã¯ãªã)  
ã»Jvmã¨ã®ã³ãã¥ãã±ã¼ã·ã§ã³ã³ã¹ãããªãï¼pythonãä»è¨èªã¨ã®å·®å¥åï¼  


##ç¬¬ï¼ç« 
Sparkã®ä»çµã¿ãç¹ã«ç®ç«ã£ãæå ±ã¯ãªããä»ã®æ¸ç±åè


##ç¬¬ï¼ç« 
###DataFrames, Datasets, and SparkSQL

SparkSQLï¼=Dataframe, Datasetsã®ã¤ã³ã¿ã¼ãã§ã¼ã¹)ã®çè§£ã¯ãããå¹ççãªã¹ãã¬ã¼ã¸ãªãã·ã§ã³ãé²æ­©çãªãªããã£ãã¤ã¶ã¼ãã·ãªã¢ã«åãããã¼ã¿ã¸ã®ç´æ¥çãªãªãã¬ã¼ã·ã§ã³ã¨å¼ã­æ·»ããSparkããã©ã¼ãã³ã¹ã®æªæ¥ã ã  
ãããã®ã³ã³ãã¼ãã³ãã¯æé«ã®ããã©ã¼ãã³ã¹ãæã«å¥ããããã«ãã£ã¡ãéè¦(super important)ã§ããã

(åè)  
åºæ¬çãªDataFrame,Datasetã¨ã¯ãªãããã«ã¤ãã¦ã¯ä»¥ä¸ãããããããã  
[Apache Sparkã®3ã¤ã®API: RDD, DataFrameããDatasetã¸] 
(https://yubessy.hatenablog.com/entry/2016/12/11/095915)

 
Spark SQLã«ã¯SparkSessionããããSparkSQLã®ã¨ã³ããªã¼ãã¤ã³ãã¨ãªã£ã¦ããã
SparkShellã®å ´åãèªåçã«sparkå¤æ°ã¨ãã¦ä½¿ç¨ãããã¨ãã§ããã
 
__Organization of entry point __  

__SparkSQL__:  org.apache.spark.sql.SparkSession  
__SparkCore__:  org.apache.spark.SparkContext


###RDDã¨ã®éã
DataFrameã¨Datasetã«ã¯ã¹ã­ã¼ãæå ±ãä»ä¸ããã¦ããããã®ã¹ã­ã¼ãæå ±ã«ãã£ã¦ã¹ãã¬ã¼ã¸ã¬ã¤ã¤ã¼å¦çã®å¹çåï¼Tungstenï¼ããªããã£ãã¤ãº(Catalyst)ã®åä¸ãå³ããã¦ããã

ä¾ãã°ãRDDã§ã®ä½¿ç¨ã«æ¸å¿µãããgroupByã ããSparkSqlã®optimaizerã®ãããã§Dataframeã§ã¯å·¨å¤§ãªã·ã£ããã«ãé¿ãå®è¡ãã©ã³ãçµã¾ãéç´å¦çããã¦ããããããå®å¨ã«å¦çãããã

è¤æ°ã®å´é¢ã§ã®éè¨ãè¤éãªéè¨ãè¡ãå ´åãç´æ¥countãªã©ãä½¿ãã®ã§ã¯ãªããaggã¡ã½ãããä½¿ç¨ããã



###Tungsten
Tungstenã¯Sparkå¦çãä½ã¬ã¤ã¤ã¼ã¬ãã«ã§å¦çå¹çä¸ããSparkSQLã®ã³ã³ãã¼ãã³ãã§ããã

Dataframeã¨Datasetããã¤specialiezed  representation(=Tungsten)ã¯ã¡ã¢ãªå¹çæ§ã®ã¿ãªãããKryoã§ãããåé§ããã·ãªã¢ã©ã¤ãºã¹ãã¼ããåºããã¨ãã§ããã

ã³ã¼ãçæãã¯ã¤ã¤ã¼ãã­ãã³ã«ãªã©Sparkã®è¦æ±ã«å¿ããããã«ãã¥ã¼ãã³ã°ãããç¹å¥ãªã¤ã³ã¡ã¢ãªãã¼ã¿æ§é ãä¿æãã¦ããã

Tungstenã¯Kryoãªã©ã«æ¯ã¹ã¦ããªãå°ãããµã¤ãºã«å§ç¸®ãã¦ãã¼ã¿ãæ±ããã¨ãã§ãããã¾ããJavaObjectã«ä¾å­ããªããããon heap, off heap allocationãæå®ã§ããã
ã¾ããå½¢å¼ãã³ã³ãã¯ãã«ãªã£ãã ãã§ãªããããã©ã¼ãã³ã¹ããã¤ãã£ãã®ã¨æ¯ã¹ããªãé«éã«ãªã£ã¦ããããã®ãããããã¯ã¼ã¯è»¢éæã®ã³ã¹ããå¤§å¹ã«æ¹åãããã

(åè)  
Datasetã§ã®è©³ããèª¬æã«ä¹ã£ã¦ããã  
[introducing-apache-spark-datasets.] 
 (https://databricks.com/blog/2016/01/04/introducing-apache-spark-datasets.html)


Thungstenã®ãã¼ã¿æ§é ã¯ãå¦çã«åªããããã¢ããã¼ã«ä½æããã¦ãããä¾ãã°å¤å¸çã«è¨ç®ã³ã¹ãã®é«ãã½ã¼ããã­ã°ã©ã ã«å¯¾ãã¦ãã§ããã  
On wireï¼a way of getting data from point to point:) representationããµãã¼ãããã¦ãããã½ã¼ããdeserializeãªãã«å®è¡ã§ããã  
(å°æ¥çã«Tungstenã¯non JVMã©ã¤ãã©ãªãããå®è¡å¯è½ã¨ãªãã¯ãã ãBLASãç·å½¢ä»£æ°ã¨ãã£ãJVMã®ã©ã¤ãã©ãªã¯ãã¼ã¿ãoff heapã¸ã®ã³ãã¼ã«å¤§åãè²»ããã¦ããã)  
å¾æ¥ã®Javaãªãã¸ã§ã¯ãã«ããã¡ã¢ãªã¼ãã¬ãã¼ã¸ã³ã¬ã¯ã·ã§ã³ã®ãªã¼ãã¼ããããé¿ãããã¨ã§ãææ¸ãã®éè¨å¦çãããå·¨å¤§ãªãã¼ã¿ã»ãããå¦çã§ããããã«ãªã£ã¦ããã  

###Dataset
Datasetã¯SparkSQLã®æ¡å¼µçã§ãåãã§ãã¯ãã³ã³ãã¤ã«æã«å®è¡ããããDatasetApiã¯å¼·åãªåcollectionã§ããã¤æ´åæ§ã¨æ©è½çãªå¤æãä½µãæã£ã¦ãããã¾ããDatasetãè«çãã©ã³ã¯Catalyst optimaizeerãæ§ç¯ããã
Datasetã¯ã³ã³ãã¤ã«æã«syntax errorã¨analysis error(åããã©ã¡ã¼ã¿ã®éã)ã®ä¸¡æ¹ããã§ãã¯ãã¦ãããã

(åè)  
Databricksã®ãã­ã°ã«è©³ããè¼ã£ã¦ããã  
[a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)

DataframeãããDatasetãä½¿ãï¼ã¤ã®çç±ã¨ãã¦ãã³ã³ãã¤ã«æã®å¼·åãªåå¤æããããDataframeã¯å®è¡æã«shemaæå ±ãæã¡åããã¦ããããã³ã³ãã¤ã«æã«ã¯shemaæå ±ã¯æ¬ ãã¦ããã
ãã®å¼·åãªåæå ±ã¯ç¹ã«ã©ã¤ãã©ãªãä½æããã¨ãã«å¹æçã§ããªããªãé¢æ°ãã¡ã½ããã«ããã¦ãè¦æ±ãããã¤ã³ãããã¨ã¢ã¦ãããããããæç¢ºã«ã§ããããã  


Datasetã®ã¢ããã³ãã¼ã¸ã¨ãªãã®ã¯Scalaã¨javaã®ã³ã¼ãã®çµ±åãå®¹æã§ãããã¨ãmap, filter, mapPartitionãªã©ã®RDDã©ã¤ã¯ãªé¢æ°ãä½¿ç¨ãããã¨ãã§ããè¿ãå¤ã¨ãã¦è¦æ±ãããåãæããã§ããã

### Catalyst

Catalyst ã¯ã¯ã¨ãªãã©ã³ã¨Sparkãèµ·åããå®è¡ãã©ã³ãç­å®ããã¯ã¨ãªãªããã£ãã¤ã¶ã¼ã§ããã
ãªã¬ã¼ã·ã§ãã«ãªå¤æï¼RDBã©ã¤ã¯ãªæ´åæ§ï¼ã¨ãã¡ã³ã¯ã·ã§ãã«ãªå¤æ(æè»ãªé¢æ°æå)ãæ¡ç¨ããããã«ã
SparkSqlã¯è«çãã©ã³ã¨å¼ã°ããã¯ã¨ãªãã©ã³ã®æ¨æ§é ãæ§ç¯ããã

DataframeãDatasetã¸ã®å¤æãéãã¦è¨­è¨ããè«çãã©ã³ã¯unresolvedè«çãã©ã³ã¨ãã¦éå§ããã  
ã¤ã¾ããspark optimaizerãè¤æ°ã®ãã§ã¼ãºã«å¦çãåãã¦ãoptimizeãéå§ããåã«åç§åãåä¸è´ãè§£æ±ºããå¿è¦ãããã  


ããããéãã¦è§£æ±ºããããã©ã³ã¯è«çãã©ã³ãå¼ã°ããsparkã¯ç´æ¥ã«è«çãã©ã³ãç°¡ç´ åããæ°ã ãæ¡ç¨ããoptimizeãããè«çãã©ã³ãçæããã  
ä¸åº¦è«çãã©ã³ãoptimizeãããã¨ãç©çãã©ã³ãçæããã
ç©çãã©ã³ã¯æé©ãªç©çãã©ã³ãä½æããããã«ã«ã¼ã«ãã¼ã¹ã¨ã³ã¹ããã¼ã¹ãç¨ãã¦ï¼Sparkã®çµé¨åã®åï¼æé©åãè¡ãã  
æé©åã¹ãã¼ã¸ã§éè¦ãªã®ã¯ããã¼ã¿ã½ã¼ã¹ã¬ãã«ã§ã®äºæ¸¬å¯è½ãªæ¼ãåºãï¼ï¼ç¡é§ãªãã®ã®æé¤ï¼ï¼  

æçµã¹ãããã¨ãã¦sparkã¯ã³ã³ãã¼ãã³ãã¸ã®ã³ã¼ãçæãæ¡æãããã³ã¼ãçæã¯Janinoãä½¿ç¨ãã¦è¡ãããã  
spark1.6ãã2.0ã«ãªã£ã¦ããããªãããã©ã¼ãã³ã¹ãåä¸ãããããã  
(åè)  
[apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)

Optimizeã®éç¨ãããããããã
[https://www.slideshare.net/maropu0804/spark-70405327](https://www.slideshare.net/maropu0804/spark-70405327)


###Conclusion
* Dataframeã®å©ç¹ã¯ãTungstenã«ããå¹ççãªã¹ãã¬ã¼ã¸ãã©ã¼ãããã¨ãCatalyst optimizerã«ããæé©åã
ä¸æ¹æ¬ ç¹ã¯ãã³ã³ãã¤ã«æã®åä»ããå¼±ããã¨ãèª¤ã£ãã«ã©ã ã¸ã®ã¢ã¯ã»ã¹ããã®ã»ãåç´ãªãã¹ã«ç¹ãã£ã¦ãã¾ãã

* Datasetã®å©ç¹ã¯ãå¼·åãªåä»ãã«ãã£ã¦Dataframenã®æ¬ ç¹ãè£ã£ã¦ããç¹ã¨ãDataframeåæ§ã®TungstenãCatalyst optimizerã®æ©æµãå¾ãããç¹ã
å¤ãã®ç¹ã§RDDã®ä»£æ¿ã¨ãªããããæ¬ ç¹ã¯æ´»çºãªéçºã«ããå°æ¥ã®å¤æ´ã«ãã£ã¦ã³ã¼ãã®æ¸ãæããå¿è¦ã¨ãªãå¯è½æ§ããããã¨ã

* RDDã®å©ç¹ã¯ãCatalyst Optimizerã«é©ããªããã¼ã¿ãä¸æã«æ±ããç¹ãæ°ããæ´æ°ããã£ã¦ãã³ã¼ãå¤æ´ãå¿è¦ãªå¯è½æ§ãä½ãç¹ãã¾ããRDDã¯ãã¼ãã£ã·ã§ãã³ã°ã®ã³ã³ãã­ã¼ã«ããããããå¤ãã®åæ£ã¢ã«ã´ãªãºã ã«æå¹ã§ããã
æ¬ ç¹ã¯ãè¤æ°ã®ã«ã©ã ã«å¯¾ããéç´ãè¤éãªjoinãªã©ã®RDD APIã§ã®è¡¨ç¾ãé£ããã
ã¾ããkryoãå©ç¨ã§ããã¨ã¯ãããDataframeãDatasetã«æ¯ã¹ã·ãªã¢ã©ã¤ãºåããé«ã³ã¹ãã«ãªãããã¤ã¡ã¢ãªå¹çããããªãã



##ç¬¬ï¼ç« 
###Join
joinã¯spark coreã«ãããspark sqlã«ããããã©ã¼ãã³ã¹ã«ããã¦éè¦ã«ãªã£ã¦ããã
Joinã¯å±éãã¦å¼·åã§ããä¸æ¹ãå·¨å¤§ãªãããã¯ã¼ã¯è»¢éãçºçããããæã«è² ããªãã»ã©ã®å·¨å¤§ãªãã¼ã¿ã»ãããä½æãã¦ãã¾ããã¨ãããã®ã§ãããã©ã¼ãã³ã¹ã«æ³¨æãã¦ããªããã°ãªããªãã
Core sparkã®å ´åã¯ãSql Optimizerã¨éã£ã¦ãªãã¬ã¼ã·ã§ã³ã®é åºãèããã®ãæéè¦ã«ãªã£ã¦ããã


###Core Spark Join  
éå¸¸Joinã¯ãå¯¾å¿ããã­ã¼ãããããã®RDDã«åããã¼ãã£ã·ã§ã³åã«ãããã¨ãè¦æ±ãããããé«ä¾¡ãªå¦çã«ãªãã  
ããRDDãpartitionãç¥ããªãã£ãããã·ã£ããã«ãå¿è¦ã¨ãªãä¸¡RDDãpartitionã®å±æãã¯ãããã  
partitionãåããã©ããã«éãããçæ¹ã®RDDãpartitionãç¥ã£ã¦ããå ´åãnarrow dependency(=ãã¼ãã£ã·ã§ã³éã®ä¾å­åº¦ãä¸ãã)ãçæãããã  
(åè)  
[Whats Narrow(Wide) Dependency ??](https://image.slidesharecdn.com/apachespark101-170216211852/95/apache-spark-101-demi-benari-35-638.jpg?cb=1487279996)  

ã»ã¨ãã©ã®Key/valueãªãã§ã¯ãã­ã¼ã®æ°ãæ­£ããpartitionãå¾ãããã«ãã©ãã«ããªããã°ãªããªãï¼ã·ã£ããã«ï¼ã¬ã³ã¼ãéã®è·é¢ã¨ã¨ãã«ã³ã¹ããå¢å¤§ãã¦è¡ãã  

Joinã®ãã¹ãã·ããªãªã¯ä¸¡æ¹ã®RDDãåãã¦ãã¼ã¯ã­ã¼ï¼éè¤ç¡ãã®ã­ã¼ï¼ã»ãããæã£ã¦ããæã  
éè¤ã­ã¼ãããã¨ãã¼ã¿ã®ãµã¤ãºãåçã«å¤§ãããªããããã©ã¼ãã³ã¹åé¡ã¨ãªãã  
ãã®ãããdistinctãcombineByKeyãªã©ã«ãã£ã¦ã­ã¼ãé åãreduceããããcogroupã«ãã£ã¦éè¤ã­ã¼ãã²ã¨ã¾ã¨ãã«ãã¦ãã¾ããã¨ã  
ã¾ããä¸¡æ¹ã®RDDã«ã­ã¼ãå­å¨ããªãå ´åããã¼ã¿ãå¤±ãå¯è½æ§ãããã  
ãã®ãããouter joinãªã©ã«ãã¦å¯¾å¦ããã

ããã©ã«ãã§ã¯shuffled hash joinãä½¿ç¨ããã  
ã ãããã¯ã·ã£ããã«ãçºçãããããå¿è¦ä»¥ä¸ã«å®è¡ã³ã¹ããé«ããªã£ã¦ãã¾ããã·ã£ããã«ãé¿ããããã«ã
ä¸¡RDDã¯ãã¼ãã£ã·ã§ã³ãç¥ã£ã¦ãããã¨ãçæ¹ã®ãã¼ã¿ã»ãããã¡ã¢ãªã¼ã«ååé©ãããããå°ãããã¨ããã®ã±ã¼ã¹ã«ããã¦ã¯broadcast hash joinãã§ããã(è©³ããã¯å¾è¿°)  

Broadcast hash joinã¯å°ããæ¹ã®RDDãããããã®ã¯ã¼ã«ã¼ãã¼ãã«ããã·ã¥ãããããã¦å¤§ããªRDDã¸map-side combineãããã  

ããçæ¹ã®RDDãã¡ã¢ãªã¼ã«é©ãã¦ããããã¡ã¢ãªã¼ã«é©ããå½¢ã«å¤æã§ããã®ã§ããã°ãã·ã£ããã«ãè¦æ±ãããªãããbroadcast hash joinã¯ã¨ã¦ãå¤§ããªå©çã¨ãªãã  

ãã ãSpark.coreã®å ´åå®è£ããªãããããã³ãã¡ã¤ãã§è¡ããªããã°ãªããªãããSpark.Sqlã«éã£ã¦ã¯ããã§ã¯ãªãã


### SparkSQL Join
Spark sqlã¯core sparkã¨åãjoinããµãã¼ããããããªããã£ãã¤ã¶-ãéçºèã®ããã«éããã®ãæã¡ä¸ãã¦ããããããªããä½åº¦ãjoinã®ã³ã³ãã­ã¼ã«ãè«¦ãã¦ãã¦ãã ï¼  ï¼ä¸è¿°ã®ãããªpartitionerã®èæ®ãªã©ãï¼  
Sparkã¯æããããjoinãå¹ççã«ãªããããªãã¬ã¼ã·ã§ã³ãå¾å¥ãååºãããããåæ§ç¯ãããããã  
ä¸æ¹ãpartitionerã®ã³ã³ãã­ã¼ã«ã¯Dataframe, Datasetã§ã¯ã§ããªãã


