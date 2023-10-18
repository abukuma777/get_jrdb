import os
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import chardet
import requests
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


def move_file(txt_file, download_folder):
    """
    指定されたテキストファイルを適切なフォルダに移動する関数。

    Parameters:
    - txt_file (str): 移動するテキストファイルの名前
    - download_folder (str): ファイルを保存する対象のフォルダ

    処理の流れ:
    1. ファイル名から年度とファイルタイプを抽出
    2. 対象のフォルダを特定
    3. フォルダが存在しない場合、作成
    4. ファイルを対象のフォルダに移動

    return:
    - None
    """
    # ファイル名から年度とファイルタイプを抽出
    match = re.search(r"([a-zA-Z]+)(\d+)", txt_file)
    if match:
        year_prefix = match.group(2)[:2]
        century_prefix = "19" if 80 <= int(year_prefix) <= 99 else "20"
        year = century_prefix + year_prefix

        # 対象のフォルダを特定
        target_folder = f"{download_folder}/{match.group(1)}/{year}/"

        # フォルダが存在しない場合、作成
        ensure_directory_exists(target_folder)

        # ファイルを対象のフォルダに移動
        shutil.move(f"tmp/{txt_file}", f"{target_folder}/{txt_file}")


def extract_and_move_file(tmp_file_path, download_folder):
    """
    ダウンロードしたファイルを解凍し、適切なフォルダに非同期で移動する関数。

    Parameters:
    - tmp_file_path (str): ダウンロードしたファイルの一時保存パス
    - download_folder (str): ファイルを保存する対象のフォルダ

    処理の流れ:
    1. ダウンロードしたファイルを解凍
    2. 解凍したテキストファイルを非同期で適切なフォルダに移動（move_file）

    注意:
    - この関数はThreadPoolExecutorを使用して非同期でファイルを移動します。

    return:
    - None
    """
    # ダウンロードしたファイルを解凍
    subprocess.run(["lha", "-xw=tmp/", tmp_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # 解凍したテキストファイルを非同期で適切なフォルダに移動
    with ThreadPoolExecutor() as executor:
        for entry in os.scandir("tmp/"):
            if entry.is_file() and entry.name.endswith(".txt"):
                executor.submit(move_file, entry.name, download_folder)


# def convert_file_to_utf8(file_path):
#     """
#     指定されたテキストファイルのエンコーディングをUTF-8に変換する関数。

#     Parameters:
#     - file_path (str): エンコーディングを変換するテキストファイルのパス

#     return:
#     - None
#     """
#     with open(file_path, "rb") as f:
#         result = chardet.detect(f.read())

#     source_encoding = result["encoding"]

#     if source_encoding.lower() != "utf-8":
#         with open(file_path, "r", encoding=source_encoding) as f:
#             content = f.read()

#         with open(file_path, "w", encoding="utf-8") as f:
#             f.write(content)


# def convert_encoding_to_utf8(folder_path):
#     """
#     指定されたフォルダおよびそのサブフォルダ内のすべてのテキストファイルのエンコーディングをUTF-8に変換する関数。

#     Parameters:
#     - folder_path (str): エンコーディングを変換するテキストファイルが保存されているフォルダのパス

#     return:
#     - None
#     """
#     txt_files = []
#     for root, dirs, files in os.walk(folder_path):
#         for filename in files:
#             if filename.endswith(".txt"):
#                 file_path = os.path.join(root, filename)
#                 txt_files.append(file_path)

#     with ThreadPoolExecutor() as executor:
#         future_to_file = {executor.submit(convert_file_to_utf8, file_path): file_path for file_path in txt_files}
#         for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Converting files"):
#             file = future_to_file[future]
#             try:
#                 future.result()
#             except Exception as exc:
#                 print(f"{file} generated an exception: {exc}")
