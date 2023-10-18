import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# sys.path.append("/app/get_jrdb/lib")
from lib_func import ensure_directory_exists
from tqdm import tqdm

# すべての列を表示する設定
pd.set_option("display.max_columns", None)


def read_and_convert_bac(file_path):
    """
    JRDBのBACファイルを読み込み、データフレームに変換する関数。

    Parameters:
    - file_path (str): 読み込むBACファイルのパス。

    Returns:
    - pd.DataFrame: BACファイルの内容を格納したデータフレーム。
    """

    # 空のリストを作成して、各行のデータを格納する
    data_list = []

    # SHIFT_JISでエンコードされたテキストファイルを開く
    with open(file_path, "r", encoding="SHIFT_JIS") as f:
        for line in f:
            # 各行をSHIFT_JISでエンコード
            byte_str = line.encode("SHIFT_JIS")
            # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
            data = {
                "場コード": byte_str[0:2].decode("SHIFT_JIS").strip(),
                "年": byte_str[2:4].decode("SHIFT_JIS").strip(),
                "回": byte_str[4:5].decode("SHIFT_JIS").strip(),
                "日": byte_str[5:6].decode("SHIFT_JIS").strip(),
                "Ｒ": byte_str[6:8].decode("SHIFT_JIS").strip(),
                "年月日": byte_str[8:16].decode("SHIFT_JIS").strip(),
                "発走時間": byte_str[16:20].decode("SHIFT_JIS").strip(),
                "距離": byte_str[20:24].decode("SHIFT_JIS").strip(),
                "芝ダ障害コード": byte_str[24:25].decode("SHIFT_JIS").strip(),
                "右左": byte_str[25:26].decode("SHIFT_JIS").strip(),
                "内外": byte_str[26:27].decode("SHIFT_JIS").strip(),
                "種別": byte_str[27:29].decode("SHIFT_JIS").strip(),
                "条件": byte_str[29:31].decode("SHIFT_JIS").strip(),
                "記号": byte_str[31:34].decode("SHIFT_JIS").strip(),
                "重量": byte_str[34:35].decode("SHIFT_JIS").strip(),
                "グレード": byte_str[35:36].decode("SHIFT_JIS").strip(),
                "レース名": byte_str[36:86].decode("SHIFT_JIS").strip(),
                "回数": byte_str[86:94].decode("SHIFT_JIS").strip(),
                "頭数": byte_str[94:96].decode("SHIFT_JIS").strip(),
                "コース": byte_str[96:97].decode("SHIFT_JIS").strip(),
                "開催区分": byte_str[97:98].decode("SHIFT_JIS").strip(),
                "レース名短縮": byte_str[98:106].decode("SHIFT_JIS").strip(),
                "レース名９文字": byte_str[106:124].decode("SHIFT_JIS").strip(),
                "データ区分": byte_str[124:125].decode("SHIFT_JIS").strip(),
                "１着賞金": byte_str[125:130].decode("SHIFT_JIS").strip(),
                "２着賞金": byte_str[130:135].decode("SHIFT_JIS").strip(),
                "３着賞金": byte_str[135:140].decode("SHIFT_JIS").strip(),
                "４着賞金": byte_str[140:145].decode("SHIFT_JIS").strip(),
                "５着賞金": byte_str[145:150].decode("SHIFT_JIS").strip(),
                "１着算入賞金": byte_str[150:155].decode("SHIFT_JIS").strip(),
                "２着算入賞金": byte_str[155:160].decode("SHIFT_JIS").strip(),
                "馬券発売フラグ": byte_str[160:176].decode("SHIFT_JIS").strip(),
                "WIN5フラグ": byte_str[176:177].decode("SHIFT_JIS").strip(),
                "予備": byte_str[177:182].decode("SHIFT_JIS").strip(),
            }
            # スペースをNaNに置換
            for key, value in data.items():
                if value == " ":
                    data[key] = np.nan

            # データリストに行データを追加
            data_list.append(data)

    # データリストからデータフレームを作成して返す
    return pd.DataFrame(data_list)


def BAC_save_to_csv():
    """
    JRDBのBACファイルを読み込み、年度ごとにCSVファイルとして保存する関数。

    この関数は、/app/data/jrdb_txt/BAC ディレクトリ内の各年度フォルダに存在する
    .txtファイルを読み込み、それらをデータフレームに変換して結合します。
    最終的に、/app/data/jrdb_csv/BAC ディレクトリに年度ごとのCSVファイルとして保存します。
    """

    # 入力と出力のディレクトリパスを設定
    base_input_directory_path = Path("/app/data/jrdb_txt/BAC")
    output_directory_path = Path("/app/data/jrdb_csv/BAC")

    # 年度のフォルダをリストに格納
    years = [f.name for f in base_input_directory_path.iterdir() if f.is_dir()]

    # 各年度のフォルダに対して処理を行う
    for year in tqdm(years):
        input_directory_path = base_input_directory_path / year
        all_dfs = []  # 各年度のDataFrameを格納するリスト

        # .txtファイルを読み込み、DataFrameに変換してリストに追加
        for filename in os.listdir(input_directory_path):
            if filename.endswith(".txt"):
                file_path = os.path.join(input_directory_path, filename)
                df = read_and_convert_bac(file_path)
                all_dfs.append(df)

        # すべてのDataFrameを結合
        final_df = pd.concat(all_dfs, ignore_index=True)

        # 出力ディレクトリが存在しない場合は作成
        ensure_directory_exists(output_directory_path)

        # 結合されたDataFrameをCSVとして保存
        final_df.to_csv(output_directory_path / f"BAC_{year}.csv", encoding="utf-8", index=False)

    return None
