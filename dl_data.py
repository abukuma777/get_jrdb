import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# TODO: Blackformatterで自作関数が自動でここにきてしまう？
from lib.lib_func import (
    download_file,
    ensure_directory_exists,
    extract_and_move_file,
    get_download_links,
    get_jrdb_user_and_password,
)
from tqdm import tqdm


def download_and_organize_jrdb_data(File_type, download_folder):
    """
    JRDBのデータをダウンロードし、指定されたファイルタイプに基づいて整理する関数。

    Parameters:
    - File_type (str): ダウンロードするデータのファイルタイプ（例：'Kab'）
    - download_folder (str): ダウンロードしたデータを保存するフォルダ

    処理の流れ:
    1. 環境変数からJRDBのユーザー名とパスワードを取得（get_jrdb_user_and_password）
    2. JRDBのWebページからHTMLを取得
    3. BeautifulSoupでHTMLを解析し、ダウンロードリンクを取得（get_download_links）
    4. ThreadPoolExecutorを使用して非同期でダウンロードリンクからデータをダウンロード（download_file）
    5. ダウンロードしたデータを一時ディレクトリに保存
    6. ダウンロードしたデータを解凍（extract_and_move_file）
    7. 解凍したデータを年度ごとのフォルダに整理（extract_and_move_file）
    8. 一時ディレクトリと一時ファイルを削除

    注意:
    - この関数は一時ディレクトリ（'tmp/'）を使用します。ディレクトリが存在しない場合、自動的に作成されます。
    - 複数の補助関数を呼び出して各処理を行います。
    - ダウンロードは非同期で行われ、高速化されています。

    return
    - None
    """

    # JRDBの基本URL
    JRDB_BASE_URL = f"http://www.jrdb.com/member/data/{File_type}/"
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
    download_links = get_download_links(soup, JRDB_BASE_URL)
    if not download_links:
        print("ダウンロードリンクが見つかりませんでした。")
        return None

    # 一時ディレクトリが存在しない場合、作成
    ensure_directory_exists("tmp")

    # ダウンロードと解凍
    with ThreadPoolExecutor() as executor:
        future_to_link = {
            executor.submit(download_file, link, (JRDB_USER, JRDB_PASSWORD)): link for link in download_links
        }
        for future in tqdm(as_completed(future_to_link), total=len(download_links)):
            link = future_to_link[future]
            try:
                tmp_file_path = future.result()
                if tmp_file_path:
                    extract_and_move_file(tmp_file_path, download_folder)
            except Exception as e:
                print(f"ダウンロード中にエラーが発生しました（{link}）: {e}")

    # 一時ディレクトリを削除
    if os.path.exists("tmp/"):
        shutil.rmtree("tmp/")

    return None
