### High Performance Spark
 
他の書籍や情報では語られていないことを中心に備忘録として  
(参考) ← 私の付け足し  

## 書籍　
### 情報
2017出版  
Spark version 2.0.1以降〜

[OReilly High Perfomance Spark](https://www.amazon.co.jp/High-Performance-Spark-Practices-Optimizing/dp/1491943203)    

  
###特徴
Pure RDDを中心にSparkの内部の動きの理解を深めていく流れ。  
読者層は中級者以上を想定しているみたいだが、初級者の方がむしろ読んだ方が良い内容かも。
パフォーマンスのボトルネックとなる要素に絞って要点を説明。
DataFrameやDatasetのRDDとの違いや利点欠点も述べている。

## 第１章 
### Why Scala
本書ではSparkApiを使う言語としてScalaを選定している。  
また、Sparkのパフォーマンスにこだわりたいなら、Scalaを使用することを強く推奨している。  
理由は以下  
・Sparkがscalaで書かれていること。  
・SparkがscalaのcollectionsApiをとてもよく真似ていることから、記法が直感的でコーディングが容易(Java7との差別化)  
・REPLsがあること。(Javaにはない)  
・Jvmとのコミュニケーションコストがない（pythonや他言語との差別化）  


## 第２章
Sparkの仕組み。特に目立った情報はない。他の書籍参考


## 第３章
### DataFrames, Datasets, and SparkSQL

SparkSQL（=Dataframe, Datasetsのインターフェース)の理解は、より効率的なストレージオプション、進歩的なオプティマイザー、シリアル化したデータへの直接的なオペレーションと兼ね添えたSparkパフォーマンスの未来だ。  
これらのコンポーネントは最高のパフォーマンスを手に入れるためにめっちゃ重要(super important)である。

(参考)  
基本的なDataFrame,Datasetとはなんぞやについては以下がわかりやすい。  
[Apache Sparkの3つのAPI: RDD, DataFrameからDatasetへ] 
(https://yubessy.hatenablog.com/entry/2016/12/11/095915)

 
Spark SQLにはSparkSessionがあり、SparkSQLのエントリーポイントとなっている。
SparkShellの場合、自動的にspark変数として使用することができる。
 
__Organization of entry point __  

__SparkSQL__:  org.apache.spark.sql.SparkSession  
__SparkCore__:  org.apache.spark.SparkContext


### RDDとの違い
DataFrameとDatasetにはスキーマ情報が付与されており、このスキーマ情報によってストレージレイヤー処理の効率化（Tungsten）、オプティマイズ(Catalyst)の向上が図られている。

例えば、RDDでの使用に懸念があるgroupByだが、SparkSqlのoptimaizerのおかげでDataframeでは巨大なシャッフルを避る実行プランが組まれ集約処理をしてくれるため、安全に処理される。

複数の側面での集計や複雑な集計を行う場合、直接countなどを使うのではなく、aggメソッドを使用する。



### Tungsten
TungstenはSpark処理を低レイヤーレベルで処理効率上げるSparkSQLのコンポーネントである。

DataframeとDatasetがもつspecialiezed  representation(=Tungsten)はメモリ効率性のみならず、Kryoでさえも凌駕するシリアライズスピードを出すことができる。

コード生成やワイヤープロトコルなどSparkの要求に応じるためにチューニングされた特別なインメモリデータ構造を保有している。

TungstenはKryoなどに比べてかなり小さいサイズに圧縮してデータを扱うことができる。また、JavaObjectに依存しないため、on heap, off heap allocationを指定できる。
また、形式がコンパクトになっただけでなく、パフォーマンスもネイティブのと比べかなり高速になっている。そのためネットワーク転送時のコストが大幅に改善される。

(参考)  
Datasetでの詳しい説明に乗っている。  
[introducing-apache-spark-datasets.] 
 (https://databricks.com/blog/2016/01/04/introducing-apache-spark-datasets.html)


Thungstenのデータ構造は「処理に優しく」をモットーに作成されており、例えば古典的に計算コストの高いソートプログラムに対してもである。  
On wire（a way of getting data from point to point:) representationもサポートされており、ソートがdeserializeなしに実行できる。  
(将来的にTungstenはnon JVMライブラリをより実行可能となるはずだ。BLASや線形代数といったJVMのライブラリはデータをoff heapへのコピーに大半を費やしている。)  
従来のJavaオブジェクトによるメモリーやガベージコレクションのオーバーヘッドを避けることで、手書きの集計処理よりも巨大なデータセットを処理できるようになっている。  

### Dataset
DatasetはSparkSQLの拡張版で、型チェックがコンパイル時に実行される。DatasetApiは強力な型collectionで、かつ整合性と機能的な変換を併せ持っている。また、Datasetも論理プランはCatalyst optimaizeerが構築する。
Datasetはコンパイル時にsyntax errorとanalysis error(型やパラメータの違い)の両方をチェックしてくれる。

(参考)  
Databricksのブログに詳しく載っている。  
[a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)

DataframeよりもDatasetを使う１つの理由として、コンパイル時の強力な型変換がある。Dataframeは実行時にshema情報を持ち合わせているが、コンパイル時にはshema情報は欠けている。
この強力な型情報は特にライブラリを作成するときに効果的で、なぜなら関数やメソッドにおいて、要求されるインプットとアウトプットをより明確にできるから。  


DatasetのアドバンテージとなるのはScalaとjavaのコードの統合が容易であること。map, filter, mapPartitionなどのRDDライクな関数を使用することもでき、返り値として要求される型が明らかである。

### Catalyst

Catalyst はクエリプランとSparkが起動する実行プランを策定するクエリオプティマイザーである。
リレーショナルな変換（RDBライクな整合性）とファンクショナルな変換(柔軟な関数指向)を採用するように、
SparkSqlは論理プランと呼ばれるクエリプランの木構造を構築する。

DataframeやDatasetへの変換を通して設計する論理プランはunresolved論理プランとして開始する。  
つまり、spark optimaizerが複数のフェーズに処理を分けて、optimizeが開始する前に参照元や型一致を解決する必要がある。  


これらを通して解決されたプランは論理プランを呼ばれ、sparkは直接に論理プランを簡素化した数だけ採用し、optimizeされた論理プランを生成する。  
一度論理プランがoptimizeされると、物理プランを生成する。
物理プランは最適な物理プランを作成するためにルールベースとコストベースを用いて（Sparkの経験則の元）最適化を行う。  
最適化ステージで重要なのは、データソースレベルでの予測可能な押し出し（＝無駄なものの排除？）  

最終ステップとしてsparkはコンポーネントへのコード生成を採択する。コード生成はJaninoを使用して行われる。  
spark1.6から2.0になってからかなりパフォーマンスが向上したらしい。  
(参考)  
[apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)

Optimizeの過程がわかりやすい。
[https://www.slideshare.net/maropu0804/spark-70405327](https://www.slideshare.net/maropu0804/spark-70405327)


### Conclusion
* Dataframeの利点は、Tungstenによる効率的なストレージフォーマットと、Catalyst optimizerによる最適化。
一方欠点は、コンパイル時の型付けが弱いこと。誤ったカラムへのアクセスやそのほか単純なミスに繋がってしまう。

* Datasetの利点は、強力な型付けによってDataframenの欠点を補っている点と、Dataframe同様のTungsten、Catalyst optimizerの恩恵を得られる点。
多くの点でRDDの代替となりうる。欠点は活発な開発による将来の変更によってコードの書き換えが必要となる可能性があること。

* RDDの利点は、Catalyst Optimizerに適さないデータを上手に扱える点。新しい更新があってもコード変更が必要な可能性が低い点。また、RDDはパーティショニングのコントロールがしやすく、多くの分散アルゴリズムに有効である。
欠点は、複数のカラムに対する集約や複雑なjoinなどのRDD APIでの表現が難しい。
また、kryoが利用できるとはいえ、DataframeやDatasetに比べシリアライズ化より高コストになり、かつメモリ効率がよくない。



## 第４章
### Join
joinはspark coreにしろ、spark sqlにしろパフォーマンスにおいて重要になってくる。
Joinは共通して強力である一方、巨大なネットワーク転送が発生したり、手に負えないほどの巨大なデータセットを作成してしまうことがあるので、パフォーマンスに注意してしなければならない。
Core sparkの場合は、Sql Optimizerと違ってオペレーションの順序を考えるのが最重要になってくる。


### Core Spark Join  
通常Joinは、対応するキーがそれぞれのRDDに同じパーティション内にあることが要求されるため高価な処理になる。  
もしRDDがpartitionを知らなかったら、シャッフルが必要となり両RDDがpartitionの共有をはじめる。  
partitionが同じかどうかに限らず、片方のRDDがpartitionを知っている場合、narrow dependency(=パーティション間の依存度が下がる)が生成される。  
(参考)  
[Whats Narrow(Wide) Dependency ??](https://image.slidesharecdn.com/apachespark101-170216211852/95/apache-spark-101-demi-benari-35-638.jpg?cb=1487279996)  

ほとんどのKey/valueオペでは、キーの数や正しいpartitionを得るためにトラベルしなければならない（シャッフル）レコード間の距離とともにコストが増大して行く。  

Joinのベストシナリオは両方のRDDが同じユニークキー（重複無しのキー）セットを持っている時。  
重複キーがあるとデータのサイズが劇的に大きくなり、パフォーマンス問題となる。  
そのため、distinctやcombineByKeyなどによってキーや領域をreduceするか、cogroupによって重複キーをひとまとめにしてしまうこと。  
また、両方のRDDにキーが存在しない場合、データを失う可能性がある。  
そのため、outer joinなどにして対処する。

デフォルトではshuffled hash joinを使用する。  
だがこれはシャッフルが発生するため、必要以上に実行コストが高くなってしまう。シャッフルを避けるために、
両RDDはパーティションを知っていること。片方のデータセットがメモリーに十分適するくらい小さいこと。このケースにおいてはbroadcast hash joinができる。(詳しくは後述)  

Broadcast hash joinは小さい方のRDDをそれぞれのワーカーノードにプッシュする。そして大きなRDDへmap-side combineをする。  

もし片方のRDDがメモリーに適しているか、メモリーに適した形に変換できるのであれば、シャッフルが要求されないためbroadcast hash joinはとても大きな利益となる。  

ただしSpark.coreの場合実装がないため、ハンドメイドで行わなければならないが、Spark.Sqlに限ってはそうではない。


### SparkSQL Join
Spark sqlはcore sparkと同じjoinをサポートするが、オプティマイザ-が開発者のために重いものを持ち上げてくれる。あなたが何度かjoinのコントロールを諦めていてもだ！  （上述のようなpartitionerの考慮など。）  
Sparkは時々、よりjoinが効率的になるようオペレーションを後入れ先出ししたり、再構築したりする。  
一方、partitionerのコントロールはDataframe, Datasetではできない。


