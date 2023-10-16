import datetime
import os
import re
import shutil
import subprocess

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")


def download_and_organize_jrdb_data(File_type, download_folder):
    """
    JRDBのデータをダウンロードし、指定されたファイルタイプに基づいて整理する関数。

    Parameters:
    - File_type (str): ダウンロードするデータのファイルタイプ（例：'Kab'）

    処理の流れ:
    1. JRDBのWebページからHTMLを取得
    2. BeautifulSoupでHTMLを解析し、ダウンロードリンクを取得
    3. tqdmを使用してダウンロードリンクからデータをダウンロード
    4. ダウンロードしたデータを一時ディレクトリに保存
    5. ダウンロードしたデータを解凍
    6. 解凍したデータを年度ごとのフォルダに整理
    7. 一時ディレクトリと一時ファイルを削除

    注意:
    - この関数は一時ディレクトリ（'tmp/'）を使用します。ディレクトリが存在しない場合、自動的に作成されます。

    return
    - None
    """

    # JRDBの基本URL
    JRDB_BASE_URL = f"http://www.jrdb.com/member/data/{File_type}/"
    print(JRDB_BASE_URL)
    # ユーザー名とパスワード（環境変数から取得）
    JRDB_USER = os.environ.get("JRDB_USER")
    JRDB_PASSWORD = os.environ.get("JRDB_PASSWORD")

    if not JRDB_USER or not JRDB_PASSWORD:
        print("環境変数でJRDB_USERとJRDB_PASSWORDを設定してください。")
        return None

    # WebページからHTMLを取得
    response = requests.get(JRDB_BASE_URL + "index.html", auth=(JRDB_USER, JRDB_PASSWORD))
    # Shift_JISで文字コードを設定
    response.encoding = "shift_jis"
    # 認証が成功したかどうかを確認
    if response.status_code == 200:
        # BeautifulSoupオブジェクトを作成
        soup = BeautifulSoup(response.text, "html.parser")
        # # ------------------------------------------
        # # soupの中身をHTMLファイルに保存（デバッグ用）
        # with open("check.html", "w") as f:
        #     f.write(soup.prettify())
        # # ------------------------------------------

        # '単体データコーナー'というテキストを含むulタグを探す
        target_ul = None
        for ul in soup.find_all("ul"):
            if "単体データコーナー" in ul.text:
                target_ul = ul
                break

        # ダウンロードリンクを取得
        if target_ul:
            download_links = [JRDB_BASE_URL + a_tag["href"] for a_tag in target_ul.find_all("a")]
        else:
            print("単体データコーナーが見つかりませんでした。")
            download_links = []

        # ダウンロードリンクが空の場合、早期リターン
        if not download_links:
            print("ダウンロードリンクが見つかりませんでした。")
            return None

        # ダウンロードと解凍
        for link in tqdm(download_links):
            response = requests.get(link, auth=(JRDB_USER, JRDB_PASSWORD))
            if response.status_code == 200:
                # 一時ディレクトリが存在しない場合、作成
                ensure_directory_exists("tmp")
                # 一時的なファイルパス
                tmp_file_path = f"tmp/{link.split('/')[-1]}"

                # ファイルを保存
                with open(tmp_file_path, "wb") as f:
                    f.write(response.content)

                # 解凍（lhaコマンドが必要、出力を無視）
                subprocess.run(["lha", "-xw=tmp/", tmp_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # 解凍したテキストファイルを整理
        for txt_file in os.listdir("tmp/"):
            if txt_file.endswith(".txt"):
                # ファイル名からアルファベットと数字を分ける
                match = re.search(r"([a-zA-Z]+)(\d+)", txt_file)
                if match:
                    # 数字部分の最初の2桁を取得
                    year_prefix = match.group(2)[:2]

                    # 年度のプレフィックスを決定（80-99なら19、それ以外は20）
                    century_prefix = "19" if 80 <= int(year_prefix) <= 99 else "20"

                    # 完全な年度を作成
                    year = century_prefix + year_prefix

                # ファイル格納フォルダを作成
                target_folder = f"{download_folder}/{match.group(1)}/{year}/"
                print(f"Target folder: {target_folder}")  # デバッグ用
                if not os.path.exists(target_folder):
                    os.makedirs(target_folder)
                    print(f"Created directory: {target_folder}")  # デバッグ用

                # ファイルを移動
                shutil.move(f"tmp/{txt_file}", f"{target_folder}/{txt_file}")
                print(f"Moved {txt_file} to {target_folder}")  # デバッグ用

            else:
                print("Status Code:", response.status_code)
                print(link)

        # 一時ディレクトリを削除
        if os.path.exists("tmp/"):
            shutil.rmtree("tmp/")

        return None

    else:
        print(f"認証に失敗しました。ステータスコード: {response.status_code}")
        print("ユーザー名とパスワードを確認してください。")
        return None  # 認証に失敗した場合、関数を終了
