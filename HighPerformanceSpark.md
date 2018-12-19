# High Performance Spark
 
他の書籍や情報では語られていないことを中心に備忘録として   
文書校正していないので訳もまぁまぁ適当です  
(参考) ← 私の付け足し  

## 書籍　
### 情報
2017出版  
Spark version 2.0.1以降〜

[OReilly High Perfomance Spark](https://www.amazon.co.jp/High-Performance-Spark-Practices-Optimizing/dp/1491943203)    

  
### 特徴
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
これらのコンポーネントは最高のパフォーマンスを出すためにめっちゃ重要(super important)である。

(参考)  
基本的なDataFrame,Datasetとはなんぞやについては以下がわかりやすい。  
[Apache Sparkの3つのAPI: RDD, DataFrameからDatasetへ](https://yubessy.hatenablog.com/entry/2016/12/11/095915)

 
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
[introducing-apache-spark-datasets.](https://databricks.com/blog/2016/01/04/introducing-apache-spark-datasets.html)


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

## 第５章
### effective transformation
Sparkがストレージ上のデータからRDD形式にデータ読み込み、コンピュテーションデータ変換をRDDで行い、結果としてRDDフォーマットにアンリ書き込みが行われるか、もしくはドライバーが寄せ集めるか。したがってSparkのパワーはRDD上で定義されRDDは返されるオペレーションに由来する。  

この章ではRDD変換、連続した変換、評価の方法を紹介する。  
because it has strong implications for how transformations are evaluated and, consequently, for their performance.  

Narrow dependencyとWide dependencyの区別は、（結果的に結果的にパフォーマンスに関わるが）どう変換が評価されるかのための強力な実装を持つため、重要である。  

簡単にまとめるとWide dependencyはシャッフルを要求し、Narrow dependencyはそうではない。  
Narrow dependencyの定義は各々の親RDDのパーティションが多かれ１つの子RDDパーティションと馴染みがあるか。  
Wide Dependencyの定義は複数の子パーティションが各々の親パーティションに依存している状態のこと。  

### Implication for performance

Narrow Dependecyはpartitionをまたがって変換がされないため、Driverとのコミュニケーションコストが発生しない。  
Wide dependencyはその逆で、machineをまたがって変換が行われる。  
実際SortByKeyはwide dependencyである。  


### Implication for falttorelance

The cost of failure for a partition with wide dependencies is much higher than for one with narrow dependencies, since it requires more partitions to be recomputed  

Wide dependencyによるパーティションの失敗コストはよりパーティションが再計算されることが要求されるため、narrow dependencyと比べ高くなる。  


### The special case coalesce
The coalesce operation is used to change the number of partitions in an RDD  
coalesceの実行はRDDのパーティション数の変更に使われる。  

coalesceが出力パーティション数をreduceするとき、各々の親パーティションは子パーティションが親パーティションのいくつかを統合したものであるため、１つ確かな子パーティションが使われる。したがってcoalesceはnarrow変換である。  

Coalesceは、シャッフルなしにpartition数を減らすことができる。しかし、coalsceはステージ全体のupstream partitionが同じ平行のレベルで実行する原因となる。いくつかのケースでは望ましくない。  
？Using coalesce, the number of partitions can decrease in one stage without causing a shuffle. However, coalesce causes the upstream partitions in the entire stage to execute with the level of parallelism assigned by coalesce, which may be undesirable in some cases. Avoid this behavior at the cost of a shuffle by setting the shuffle argument of coalesce to true or by using the repartition func‐ tion instead.  

GCのエラーは失敗の共通原因となる。オブジェクトのサイズを小さくしたり、数を減らしたり、再利用するなどしてGCのオーバーヘッドを防ぐこと。  
シリアライズエラーや不正確な結果に繋がるため、ミュータブルなデータは避けることがベストである  

mapPartition変換はsparkのなかでもっとも強力だ。ユーザーがパーティション上のに任意のルーチンを提議させることができるため。  
mapPartition変換はとてもシンプルなデータ変換に使われるが、セカンダリーソートなどの問題を解決するための複雑な、高価なデータ処理にも使われる。  

Scalaのiteratorオブジェクトは実際にはコレクションではないが、collectionの要素へ１つ１つアクセスする処理を定義する関数である。  
iteratorがimmutableであるだけでなく、iteratorの同じ要素は１度だけアクセスできる。  

つまり、iteratorは１度だけtraversedされる。(scalaのTravesableOnceインターフェースを継承している。)  
イテレータは保存された状態というよりは実際には評価指標のセットである。  
an iterator is actually a set of evaluation instructions rather than a stored state  
結果的に、iterator to iterator変換はSparkがメモリエラーなしに1つのexecutor上のメモリにfitするには大きすぎるパーティションを巧みに扱うことができる。  
さらに、iteratorとしてpartitionをキープすることはsparkが使うdiskスペースをより選択的にすることができる。  
(Furthermore, keeping the partition as an iterator allows Spark to use disk space more selectively)  



## 第６章
### working with key/vakue data
Key valueオペレーションはいくつかパフォーマンス問題に繋がる。ほとんどwide transoformationがkey/value変換がもっとも良いチューニング必要で、振る舞いに気をつることを要求することため、key value変換は実際もっとも高くつくオペレーションである。  

(In particular, operations on key/value pairs can cause: Out-of-memory errors in the driver: Out-of-memory errors on the executor nodes :Shuffle failures :“Straggler tasks” or partitions, which are especially slow to comput)  

特に、key valueオペレーションは、driverやexecutor nodeのout of memoryエラーやシャッフルの失敗、タスクやパーティションがはぐれたりする原因となる。  

処理の中で、何度か変換を要求するシャッフルの回数を小さくする１つの方法は、再シャッフルを避けるためnarrow変換をまたがってパーティショニングが保持されていることを明確にすること。  
(One way to minimize the number of shuffles in a computation that requires several transformations is to make sure to preserve partitioning across narrow transformations to avoid reshuffling data)  

→(多分) 再シャッフルを避けるために、Narrow変換が各partitionで処理されるようRDDがパーティショニングされていることを確認すること。  

時にはシャッフルなしにコンピュテーションを終了することができない。その際にも全てのレコードをメモリーに乗せ、wide変換をするのではなく、reduceByKeyやaggregateByKeyなどのmap-side reductionするwide変換を使用することで、メモリーエラーを防ぎ、スピードアップすることができる。  


### GroupByKeyの危険性
小さいデータの上で、特に入力データにカラムが多くあるが、レコード数は少ない場合では比較的効果的に振る舞う。なぜなら、たった１度だけしかシャッフルが発生しないから、そしてソートがexecutor上のnarrow変換として処理されるためである。１万レコードと２、３千カラムレベルの話ではかなり有効であるが、２、３百万レコードの場合、結果的にmemory execptionで落ちるであろう。  
このようにスケール上でmemory errorを引き起こす原因として、分散処理ができないiterarorであることが理由にある。  
全てのデータをdiskから読み込み、memory上に載せようとして高価な処理となってしまう。  

通常、shuffleが走る前にキーの数を減らすmap-side reductionをするaggreagate処理をするのがより良い選択である。  
その代表格がcombineByKeyである。  
combineByKeyや全ての集約オペレーションがgroupByKeyよりもmemory errorの観点で優れているものはない。  

### Co-Grouping
全てのアキュムレータ処理に共通してcombinByKeyを使う実装がされており、全てのjoinはcogroup関数を使用している。  
Cogroupは複数のRDDをjoinする際に、joinの代替として有効である。  
だがそのような利点を持っているにもかかわらず、groupByKeyと同様の理由でmemory errorを引き起こす。  

概念的にparitionerはどれほどレコードが分散さているかを定義する。したがって、レコードは各々のtaskで完了する。  
getPartitionはキーからパーティションの数値インデックをマッピングする。  

### Goldlicks example
ここで扱うデータはカラム情報を保持しているためSparkSqlを使用することが先に思いつくだろう。  
だが、SparkSqlではランク集計はサポートされていないため、このケースではSparkCoreを使用する。  
もちろん、SparkSqlでも実装は可能だがそれでもSparkCoreの方がより良い結果を出してくれる。  

### Goldlicks version:0

最初のアプローチはiterativelyにそれぞれのグループをループし、分散ソートを実行する。その結果１ステージに１高負荷の分散ソートがグループごとに行われる。  
カラムごとにソート処理を噛ませるプログラムになっており、カラム数が増えればソートの回数も増えとても遅くなっていくことがネックになる事例  


### Goldlicks version:1
次のアプローチは、同じパーティションに同じグループを配置して、groupByKeyを使ってレコードをシャッフルすること。  
そして、我々はそれぞれのグループの値をmapPartitionを通してソートすることで、１つのステージでそれぞれのグループのソートが可能となった。  
groupByKeyを使用して比較的簡素なコードとなって、かつ正しい結果が帰ってくることがある程度補償された方法である。だが、groupByKeyの危険性での理由からあまり良い方法とは言えない結論に。  

### Goldlicks version:2
セカンダリーソートテクニックを使用すると、groupByKeyとrepartitionAndSortWithinPartitions関数を使用したソート、シャッフルステージの最中にそれぞれのグループをsortする作業をプッシュする、と入れ替えることでgroupByKeyソリューションを改善した。  
だが、もしカラムが比較的長いと、repartitionAndSortWithinPartitions ステップは失敗につながる。なぜなら１つのエグゼキュータに全てのカラムが持つ同じハッシュ値に結びつく全ての値を保持することを要求するためである。  

repartitionAndSortWithinPartitions  
repartitionがpartitionごとにグループを作り、パーティションごとにsortプログラムを走らせる。  

### Goldlicks version:3
次に、グループごとというよりはそれぞれのレコードの値上で一度だけsortする問題を解消することが可能であると発覚した。  
値ごとのレコードをキーにする、全てのレコードをソートする、そして結果を集めるためのnarrow変換のシリーズを実行する。  
Version2でキーとして使用したそれぞれのグループのサイズよりかは、重複ができるだけ含まれていないことを我々は新しくソートするキーに対して想定している。  

### Goldlicks version:4
最終的に、それぞれのグループでレコードの超複数が高いことに気づき、ソートする前にmap-side reductionをソリューションの前に実施した。  
このソリューションの結果は我々の入り込んだクライアントデータに良い結果をもたらした。  
結局はwide変換を使用せず、Narrow変換をpartitionごとにsortやdistinct処理をして最終的にcollect()することが最良としたコードだった。  


### 変換する際の重要点の整理
* Narrow 変換は素早くそして並行化しやすい。
* Wide 変換はたくさんのユニークキーがある場合に有効である。
* sortByKeyはpartitionに対して実行する分には良い。
* mapPartitionは全パーティションがメモリーにロードされることを防ぐ。

入力partition上でグルーピングすることで重複キーを集約するnarrow transformationwは、重複する値がカラムにある場合のみ有効であって、全てがユニークな値の場合、このオペレーションはなんの利益もなくメモリーエラーを引き起こす原因となる、  

