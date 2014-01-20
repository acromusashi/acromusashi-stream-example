## 概要
acromusashi-stream-exampleプロジェクトはacromusashi-streamを利用して作成した  
サンプルプログラム群の環境構築方法／利用方法についてまとめたものです。  

## スタートガイド
### デプロイ
acromusashi-stream-example の各種機能を動作させるためには、以下の手順が必要です。
- Step1: Storm／必要となるミドルウェアのインストール
- Step2: acromusashi-stream-example のデプロイ
- Step3: Topologyの起動

#### Step1: Storm／必要となるミドルウェアのインストール
Stormのインストールについては[Stormのインストール](https://github.com/acromusashi/acromusashi-stream#step1-storm%E3%81%AE%E3%82%A4%E3%83%B3%E3%82%B9%E3%83%88%E3%83%BC%E3%83%AB)を確認してください。  
ミドルウェアのインストールについてはExamplesの各ページを確認してください。
#### Step2: acromusashi-stream-example のデプロイ
GitHubからソースコードをダウンロードし、下記のコマンドを実行してビルドを行います。  
```
# mvn clean package
```
生成されたacromusashi-stream-example.zipを用いてデプロイを行います。
デプロイ手順は[Step2: acromusashi-stream を利用して開発したTopologyのデプロイ](https://github.com/acromusashi/acromusashi-stream#step2-acromusashi-stream-%E3%82%92%E5%88%A9%E7%94%A8%E3%81%97%E3%81%A6%E9%96%8B%E7%99%BA%E3%81%97%E3%81%9Ftopology%E3%81%AE%E3%83%87%E3%83%97%E3%83%AD%E3%82%A4)を確認してください。  
acromusashi-stream-exampleの場合、「開発したTopologyのjar」は「acromusashi-stream-example-x.x.x.jar」となります。  
#### Step3: Topologyの起動
起動手順はExamplesの各ページを確認してください。

## Examples
下記のWikiページを確認してください。  
- <a href="https://github.com/acromusashi/acromusashi-stream-example/wiki/Abstract">Example構成概要</a>
- <a href="https://github.com/acromusashi/acromusashi-stream-example/wiki/Run-in-Single-Process">シングルプロセスでの利用方法</a>
- <a href="https://github.com/acromusashi/acromusashi-stream-example/wiki/Camel-Usage">Camelの利用方法</a>
- <a href="https://github.com/acromusashi/acromusashi-stream-example/wiki/Kestrel-Usage">Kestrelの利用方法</a>
- <a href="https://github.com/acromusashi/acromusashi-stream-example/wiki/SnmpToolkit-Usage">SnmpToolkitの利用方法</a>

## ダウンロード

## License
This software is released under the MIT License, see LICENSE.txt.
