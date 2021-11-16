# SparkTips

- ある程度Spark触っていて、パフォーマンスが伸びない時に
- Sparkを始めるにあたって、引っかかりポイントを抑えておきたい時に


# まずSparkとその仕組みついて
- 並列分散処理のフレームワーク
- Hadoopの弱点を補完する目的として開発

## 並列分散処理について
### folk-joinモデル
- 複数のスレッド(or プロセス)で、異なるデータ群に対して同一の処理を実行する方法。最後に１つに結果をまとめる
  - 代表的なところといえばマージソートや、AkkaのDispatcherに利用されている(Actorを並列で実行させるようなもの)

- 例えば
  - テキスト処理である文に対して分かち書きをしたいとき
    - 対処の文章をいくつかのスレッドに分けて（=マルチスレッド）
    - 最後に結果を１つにまとめて結果を得る

- 擬似コード
  ```
    let gather = Vec::new()
    let fixed = Vec::new()
    for value in vector(1,2,3,4 ...) {
        let wait_result = thread::spawn(move || {
          wakati_write(value)
        });  

        gather.push(wait_result)
    }

    for wait in gather {
        let result = wait.join().unwrap();
        fixed.push(result)
    }


  ```

- ポイント
  - データセットをいくつかのデータ群に分けることが可能で
  - 同一の処理をそれぞれのデータ群に噛ませることで、全体の結果として不整合が起きないときに有効
    - つまり、複数に分けたデータへの処理がそれぞれ独立していることが条件
  - ただ現実には異なるDBのJoinなど異なるデータ群でJoinが必要だったり、データの中身によって処理を分けなければいけない場面が多い。


### MapReduce
- 巨大なデータセットに対して、多数のコンピュータの集合であるクラスタで並列処理を実現する仕組み
  - folk joinを1コンピューター・1 OSではなく、複数のコンピュータに拡張したようなもの
    - 多数のサーバを持てばもつほど、扱うデータ量が増える
    - Hadoop/Sparkが採用している
- 基本はMap(変換) -> Reduce(集約)を実行単位としている
  - サーバー間でデータが散らばってしまうためにそれらのデータをまとめて整理するShuffleがあるのも特徴


- ポイント
  - 1コンピューターでは処理しきれない or 時間がかかり過ぎてしまうデータ量を扱うときに
  - 多数サーバーを用意できて、サーバーを増やす分だけスケールする処理のときに有効
  - shuffleによって（特定のKeyでデータを集約する）、Reduce後にデータ全体の整合性を取ることが可能



## HadoopとSparkの関係について
### Hadoop
- MapReduceを実行可能にしたフレームワーク
- fileIOを基本として（Hdfs）、MapやReduceの度にデータをストレージに読み書きしていく
- MapとReduceの反復作業のため、つねにこのパターンに落とし込む必要がある

### Spark
- HadoopのfileIOによるボトルネックを、メモリにデータを乗せたデータセット(RDD)を扱うことで処理の高速化を実現した
  - HadoopのようにReduceした結果をDiskへ書き込む事なくインメモリの状態で、更に次のMap処理をそのデータセットに対して行なうことが可能
- Map -> Reduceの反復ではなく、変換の連続 Map -> Map -> Map -> Reduceといった処理をより柔軟に実装できるようになった 

## 関連
- どちらもMapReduceを基本とした処理系から発展したフレームワーク
- 複数のコンピューター上で動くことと処理がスケールすることが前提
- SparkはHadoopのMapReduceがFileIOが前提であること、Map -> Reduceに処理が固定されて柔軟ではなかったことをRDDによって解消したフレームワーク



# Sparkのデータ型 RDD/DataFrame/Dataset
- 違いはいろんな記事がWebにあるので割愛
- ここではポイントだけ

## Catalyst
- 変換からアクションまでの実行プランを最適化してくれるOptimizer
  -groupBy/aggなどで巨大なシャッフルが起きづらいようになっている
  - dataframe/datasetで利用可能

## Tungsten
- JVMを介さないシリアライズ・デシリアライズの仕組み
  - JavaObjectを利用しないためGCなどのオーバーヘッドが発生しづらくなっている
  - 従来のKryoなどよりも高速らしい
  - dataframe/datasetで利用可能

### シリアライズ・デシリアライズについて
- Sparkでは複数のコンピューター上で動くことが前提となっているため、データとその処理が複数のサーバーでまたがる
  - Driverという仕組みが全体の統合をする役割で、１サーバーに割り当てられることが多い
  - Workerという仕組みがDriverから指令を受けて処理を実行する
- サーバー間でデータのやりとりのため、NetworkIOが発生する
  - Networkに適した形にデータを変換することをシリアライズ、Sparkで処理するデータに変換することをでシリアライズ
- shuffleなどNetworkIOが多発する場面において、頻繁にシリアライズ・でシリアライズは発生するためいかにコストを下げるかがパフォーマンス向上の課題となる
  - サーバーを増やす場合、NetworkIOコストが高まるためスケールの観点からも重要になりうる

## Scala/Javaコードとの統合
- RDDはScala(Java)で実装されている
- DataFrame/DatasetはRDDによって構築されている
- map/flatMap/filter/mapPartionsなどでScala/Javaのコードを組み込める
  - RDD/Daatasetで利用可能

## Narrow Transformation
- Narrow Dependency(変換の際にシャッフルを必要としない)の状態で変換を重ねていくこと
  - Partition間でのDependencyを下げる変換をしていく
    - 1 Partition内のレコードを減らす
      - 重複削除、畳み込みでレコードを束ねて減らす
  - mapPartitionsやcoalesceがそれに当たる  

### Narrow/Wide Dependency
- Narrow Dependency
  - 変換の際にシャッフルを伴わないパーティションの状態のこと
  - 処理Cost低
- Wide Dependency
  - 変換の際にシャッフルが発生するパーティションの状態のこと
  - 処理Cost高
    - 分散処理のパフォーマンスを上げるためには、如何にWide Dependencyの状態にならないように制御することが必要

### mapPartitions
- partitionにたいして任意の処理を定義できる
  - shuffleは発生しないNarrow Transformation
- partitionはScalaのIterator Objectで抽象化されているため、Scala・Javaコードの組み込みが容易
  - pySparkの場合、JVMとのコミュニケーションコストが発生する
  - Iterator to Iteratorとなるため、Executorが抱えられるメモリ容量をオーバーせずに実行可能
  - 単純な変換も可能だが、ScalaのSeqといったcollectionに変換して、Scalaで表現可能な範囲で複雑な変換処理も実装可能
    - 複雑なJsonの解析や、toSeq.groupByとmaxByを使ってtimestampから最新のレコードのみを取得するといった実装も可能
    - joinも実装可能
    - SQLだけでは表現の難しい処理をコーディングすることが可能
- 課題に対して変換を多用すること、複雑な変換が必要な場合に有効
  - HighPerformanceSparkの著者も重宝しているらしい

## 結論
- shuffleなどのNetworkIOコストをDataframe or DatasetのOptimizerで抑えつつ、Narrow Transformationの観点からmapPartitionsを選択肢として使えるDatasetがベター
  - そのためDataFrameがベストなチョイスとなるpySparkは一旦は選択肢から外れる
    - 構造化されたデータのみを扱う or Wide TransformationのCostが低いことがあらかじめわかっているならベター
    - ScalaでもDataframeの利用は可能なため、扱うデータと実装する処理に応じて後から調整すればよい
  - 必要になればRDD/DataFrameへの変換は容易に可能

