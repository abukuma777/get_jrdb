import datetime
import os
import re
import shutil
import subprocess

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def ensure_directory_exists(directory):
    """
    指定されたディレクトリが存在するか確認し、存在しない場合は作成する関数。

    Parameters:
    - directory (str): 確認または作成するディレクトリのパス

    処理の流れ:
    1. 指定されたディレクトリが存在するか確認
    2. 存在しない場合、ディレクトリを作成

    注意:
    - ディレクトリが作成された場合、その旨がコンソールに出力されます。

    return
    - None
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")


def get_jrdb_user_and_password():
    """
    環境変数からJRDBのユーザー名とパスワードを取得する関数。

    return:
    - JRDB_USER (str): JRDBのユーザー名
    - JRDB_PASSWORD (str): JRDBのパスワード
    """
    JRDB_USER = os.environ.get("JRDB_USER")
    JRDB_PASSWORD = os.environ.get("JRDB_PASSWORD")
    if not JRDB_USER or not JRDB_PASSWORD:
        print("環境変数でJRDB_USERとJRDB_PASSWORDを設定してください。")
        return None, None
    return JRDB_USER, JRDB_PASSWORD


def get_download_links(soup, JRDB_BASE_URL):
    """
    BeautifulSoupオブジェクトからダウンロードリンクを抽出する関数。

    Parameters:
    - soup (BeautifulSoup): JRDBのWebページのHTMLを解析したBeautifulSoupオブジェクト
    - JRDB_BASE_URL (str): JRDBの基本URL

    return:
    - list: ダウンロードリンクのリスト
    """
    target_ul = None
    for ul in soup.find_all("ul"):
        if "単体データコーナー" in ul.text:
            target_ul = ul
            break
    if target_ul:
        return [JRDB_BASE_URL + a_tag["href"] for a_tag in target_ul.find_all("a")]
    else:
        print("単体データコーナーが見つかりませんでした。")
        return []


def download_file(link, auth):
    """
    指定されたURLからファイルをダウンロードする関数。

    Parameters:
    - link (str): ダウンロードするファイルのURL
    - auth (tuple): 認証情報（ユーザー名, パスワード）

    return:
    - str: ダウンロードしたファイルの一時保存パス
    """
    response = requests.get(link, auth=auth)
    if response.status_code == 200:
        tmp_file_path = f"tmp/{link.split('/')[-1]}"
        with open(tmp_file_path, "wb") as f:
            f.write(response.content)
        return tmp_file_path
    else:
        print("Status Code:", response.status_code)
        print(link)
        return None


def extract_and_move_file(tmp_file_path, download_folder):
    """
    ダウンロードしたファイルを解凍し、指定されたフォルダに移動する関数。

    Parameters:
    - tmp_file_path (str): ダウンロードしたファイルの一時保存パス
    - download_folder (str): ファイルを保存する対象のフォルダ

    return:
    - None
    """
    subprocess.run(["lha", "-xw=tmp/", tmp_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for txt_file in os.listdir("tmp/"):
        if txt_file.endswith(".txt"):
            match = re.search(r"([a-zA-Z]+)(\d+)", txt_file)
            if match:
                year_prefix = match.group(2)[:2]
                century_prefix = "19" if 80 <= int(year_prefix) <= 99 else "20"
                year = century_prefix + year_prefix
                target_folder = f"{download_folder}/{match.group(1)}/{year}/"
                ensure_directory_exists(target_folder)
                shutil.move(f"tmp/{txt_file}", f"{target_folder}/{txt_file}")


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
    4. tqdmを使用してダウンロードリンクからデータをダウンロード（download_file）
    5. ダウンロードしたデータを一時ディレクトリに保存
    6. ダウンロードしたデータを解凍（extract_and_move_file）
    7. 解凍したデータを年度ごとのフォルダに整理（extract_and_move_file）
    8. 一時ディレクトリと一時ファイルを削除

    注意:
    - この関数は一時ディレクトリ（'tmp/'）を使用します。ディレクトリが存在しない場合、自動的に作成されます。
    - 複数の補助関数を呼び出して各処理を行います。

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
    for link in tqdm(download_links):
        tmp_file_path = download_file(link, (JRDB_USER, JRDB_PASSWORD))
        if tmp_file_path:
            extract_and_move_file(tmp_file_path, download_folder)

    # 一時ディレクトリを削除
    if os.path.exists("tmp/"):
        shutil.rmtree("tmp/")

    return None
