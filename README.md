# Dataflow Demo

## 概要
指定されたGCSへのファイル格納をトリガーに、

DataFlowで簡単なデータクレンジング および
BigQueryへのロードを行います。

ファイル格納検知、DataFlow起動は
CloudFunctionsを利用しています。

[DataFlow 参考ページ](https://cloud.google.com/dataflow/?hl=ja)

[CloudFunctions 参考ページ](https://cloud.google.com/functions/?hl=ja)

* 以下のケースにて利用が推奨されます。
  * データ量が爆発的に増加により、ローカルPC 1台では定期バッチが指定時間以内に終わらない
  * クレイジング処理の複雑化など計算量の増加により、ローカルPCの動作が不安定

* 今回のsampleでは、INTEGER型のColumnに それ以外の型が混入した場合（`aaa`など）、`0`を代入する処理になっています。

## DataFlow

1. DataFlow APIの有効化

https://console.cloud.google.com/apis/api/dataflow.googleapis.com/overview?project= [your Project id]

2. create template

```
cd dataflow
```
```
mvn compile exec:java -Dexec.mainClass=com.example.LoadBigQuery \
-Dexec.args=\
"--project=<< your project id >> \
--stagingLocation=<< staging location >> \
--runner=TemplatingDataflowPipelineRunner \
--inputFile=<< input file >> \
--bigQueryDataset=<< target dataset >> \
--dataflowJobFile=<< template file >>"
```

* 指定するバケットは先に作成されている必要があります

* stagingLocation 
  * dataflow実行ファイル(jar)の格納する場所の指定してください
  * 例 `gs://example/stg`

* inputFile
  * 入力Fileが格納されている場所を指定してください
  * 例 `gs://example-input/test.csv`
    * cloud functionsで指定する監視バケットの配下を指定してください

* bigQueryDataset
  * 出力先のDatasetを指定してください
  * 例 `test`
    * 出力先のデータセットは先に作成されている必要があります
    * テーブルに関しては作成不要です

* dataflowJobFile
  * dataflowのtemplateを格納する場所を指定してください
  * templateを参照して、使い捨ての実行ファイルが作成される
  * 例 `gs://example/templates/LoadBigQuery`

## cloud functions

1. cloudfunction APIの有効化
https://console.cloud.google.com/apis/api/cloudfunctions.googleapis.com/overview?project=[your Project id]

2. Edit config.json
config.jsonを編集して下さい。

* PROJECT_ID
  *  Project IDを記入して下さい
* DATAFLOW_TEMPLATE_PATH 
  *  dataflowJobFileで指定したPATHを記入してください
  *  例 `gs://example/templates/LoadBigQuery`

2. deploy cloud functions

```
cd functions
```
```
npm install && npm run build
```
```
gcloud beta functions deploy dataflowJob --stage-bucket << stage bucket >> --trigger-bucket << trigger bucket >>
```
* stage-bucket
  * dataflowの実行ファイル(.js)が格納される場所を指定してください
`gs://example`

* trigger-bucket
  * 監視対象のバケットを指定してください
  * このバケットに変更があると、デプロイしたjsが起動
  * 各実行ファイルと同じバケットにすると、デプロイの度に起動するため独立したバケットが推奨されている
    * ここに格納されたfileを指定されたデータセットに格納します。
`gs://example-input`

## Run

* copy to cloud storage

```
gsutil cp test.csv << trigger bucket >>
```

* watch cloud functions logs

```
gcloud beta functions logs read --limit 50
```

