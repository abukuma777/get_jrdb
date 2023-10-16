# get_jrdb

## 概要

このレポジトリは、[JRDB](http://www.jrdb.com/)が提供する競馬データをダウンロードするためのPythonスクリプトを提供しています。データの利用にはJRDBの会員登録が必要であり、Basic認証のためのユーザーIDとパスワードが必要になります。

- [JRDB 会員登録ページ](http://www.jrdb.com/order.html)

ダウンロードされるファイルは、文字コードが`CP932`で、固定長のテキストファイル（.txt）形式です。必要に応じて、文字コードやファイル形式の変換を行ってください。

## 初期設定

### 環境変数の設定

このスクリプトでは環境変数の管理に`direnv`を使用しています。`direnv`がインストールされていない場合は、以下のコマンドでインストールしてください。

```bash
# direnvのインストール
sudo apt-get update
sudo apt-get install direnv
```

次に、.envrcファイルを作成し、必要な環境変数を定義します。JRDB_USERとJRDB_PASSWORDは、JRDBの会員登録時に取得した情報を使用してください。


```bash
# .envrcファイルの内容
export JRDB_USER=your_username
export JRDB_PASSWORD=your_password
```

環境変数を有効にするには、以下のコマンドを実行します。

```bash
# 環境変数をロード
direnv allow .
# bashの設定ファイルにdirenvの設定を追加
echo 'eval "$(direnv hook bash)"' >> ~/.bashrc
source ~/.bashrc

```
