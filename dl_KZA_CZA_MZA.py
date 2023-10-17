import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup
from lib.lib_func import (
    download_file,
    ensure_directory_exists,
    extract_and_move_file,
    get_jrdb_user_and_password,
)


def download_jockey_data(download_folder):
    """
    JRDBのKZA, CZA, MZAをダウンロードし、整理する関数。


    Parameters:
    - download_folder (str): ダウンロードしたデータを保存するフォルダ

    処理の流れ:
    1. 環境変数からJRDBのユーザー名とパスワードを取得
    2. JRDBのWebページからHTMLを取得
    3. BeautifulSoupでHTMLを解析し、ダウンロードリンクを取得
    4. ダウンロードリンクからデータをダウンロード
    5. ダウンロードしたデータを一時ディレクトリに保存
    6. ダウンロードしたデータを解凍
    7. 解凍したデータを整理
    8. 一時ディレクトリと一時ファイルを削除

    return:
    - None
    """

    # JRDBの基本URL
    JRDB_BASE_URL = "http://www.jrdb.com/member/data/"
    print(JRDB_BASE_URL)

    # 環境変数からJRDBのユーザー名とパスワードを取得
    JRDB_USER, JRDB_PASSWORD = get_jrdb_user_and_password()
    if not JRDB_USER or not JRDB_PASSWORD:
        print("環境変数でJRDB_USERとJRDB_PASSWORDを設定してください。")
        return None

    # WebページからHTMLを取得
    response = requests.get(JRDB_BASE_URL + "index.html", auth=(JRDB_USER, JRDB_PASSWORD))
    response.encoding = "shift_jis"

    # 認証が成功したかどうかを確認
    if response.status_code != 200:
        print(f"認証に失敗しました。ステータスコード: {response.status_code}")
        print("ユーザー名とパスワードを確認してください。")
        return None

    # BeautifulSoupオブジェクトを作成
    soup = BeautifulSoup(response.text, "html.parser")

    # ダウンロードリンクを取得
    target_data_types = ["JRDB騎手データ(KZA)", "JRDB調教師データ(CZA)", "JRDB抹消馬データ(MZA)"]

    download_links = []
    for tr in soup.select("tr"):
        td_elements = tr.select("td")
        if len(td_elements) >= 3 and td_elements[2].text in target_data_types:
            lzh_link = td_elements[3].select_one('a[href$=".lzh"]')
            if lzh_link:
                download_links.append(JRDB_BASE_URL + lzh_link["href"])
    print(download_links)

    # 一時ディレクトリが存在しない場合、作成
    ensure_directory_exists("tmp")

    # ダウンロードと解凍
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(download_file, link, (JRDB_USER, JRDB_PASSWORD)) for link in download_links]

        for future in futures:
            tmp_file_path = future.result()
            if tmp_file_path:
                extract_and_move_file(tmp_file_path, download_folder)

    # 一時ディレクトリを削除
    if os.path.exists("tmp/"):
        shutil.rmtree("tmp/")

    return None
