# get_jrdb

## 概要

このレポジトリは、[JRDB](http://www.jrdb.com/)が提供する競馬データをダウンロードするためのPythonスクリプト。<br>
データの利用にはJRDBの会員登録が必要であり、Basic認証のためのユーザーIDとパスワードが必要である。<br>

- [JRDB 会員登録ページ](http://www.jrdb.com/order.html)

ダウンロードされるファイルは、文字コードが`CP932`で、固定長のテキストファイル（.txt）形式。必要に応じて、文字コードやファイル形式の変換を行う。<br>

## 初期設定

### 環境変数の設定

このスクリプトでは環境変数の管理に`direnv`を使用する。`direnv`がインストールされていない場合は、以下のコマンドでインストールする。<br>

```bash
# direnvのインストール
sudo apt-get update
sudo apt-get install direnv
```

次に、.envrcファイルを作成し、必要な環境変数を定義する。JRDB_USERとJRDB_PASSWORDは、JRDBの会員登録時に取得した情報を使用する。<br>


```bash
# .envrcファイルの内容
export JRDB_USER=your_username
export JRDB_PASSWORD=your_password
```

環境変数を有効にするには、以下のコマンドを実行する。<br>

```bash
# 環境変数をロード
direnv allow .
# bashの設定ファイルにdirenvの設定を追加
echo 'eval "$(direnv hook bash)"' >> ~/.bashrc
source ~/.bashrc

```

## 基本的な使い方

以下のコマンドで、下記の処理ができます。<br>
処理時間は、全て一括で行うと数時間かかります。<br>

1. DL->解凍->txt
2. txt(エンコードバラバラ)->CSV(utf-8)

```bash
python main.py
```
### 環境変数とスクリプトの関連性

環境変数は `direnv` と共に用いられ、`main.py` や他のスクリプトでの認証などに使用。
