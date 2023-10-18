import os
from pathlib import Path

import numpy as np  # np.nanを使用するために追加
import pandas as pd
from lib_func import ensure_directory_exists, hex_to_dec
from tqdm import tqdm


class JRDBFileConverter:
    """
    JRDBの特定のファイルタイプ（BAC、CZAなど）をCSVに変換するクラス。

    Attributes:
    - file_type (str): 変換するファイルのタイプ（例：'BAC', 'CZA'）。
    - encoding (str): ファイルのエンコーディング（例：'SHIFT_JIS', 'UTF-8'）。
    """

    def __init__(self, file_type, encoding):
        """
        コンストラクタ。

        Parameters:
        - file_type (str): 変換するファイルのタイプ。
        - encoding (str): ファイルのエンコーディング。
        """
        self.file_type = file_type
        self.encoding = encoding

    def read_and_convert(self, file_path):
        """
        指定されたファイルタイプに応じて、ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むファイルのパス。

        Returns:
        - pd.DataFrame: 変換されたデータフレーム。
        """
        if self.file_type == "BAC":
            return self.read_and_convert_bac(file_path)
        elif self.file_type == "CZA":
            return self.read_and_convert_cza(file_path)
        elif self.file_type == "JOA":
            return self.read_and_convert_joa(file_path)
        elif self.file_type == "KZA":
            return self.read_and_convert_kza(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        # elif self.file_type == "JOA":
        #     return self.read_and_convert_joa(file_path)
        else:
            raise ValueError("Unsupported file type")

    def read_and_convert_bac(self, file_path):
        """
        BACファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むBACファイルのパス。

        Returns:
        - pd.DataFrame: BACファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []

        # SHIFT_JISでエンコードされたテキストファイルを開く
        with open(file_path, "r", encoding=self.encoding) as f:
            for line in f:
                # 各行をSHIFT_JISでエンコード
                byte_str = line.encode(self.encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": byte_str[0:2].decode(self.encoding).strip(),
                    "年": byte_str[2:4].decode(self.encoding).strip(),
                    "回": byte_str[4:5].decode(self.encoding).strip(),
                    "日": byte_str[5:6].decode(self.encoding).strip(),
                    "Ｒ": byte_str[6:8].decode(self.encoding).strip(),
                    "年月日": byte_str[8:16].decode(self.encoding).strip(),
                    "発走時間": byte_str[16:20].decode(self.encoding).strip(),
                    "距離": byte_str[20:24].decode(self.encoding).strip(),
                    "芝ダ障害コード": byte_str[24:25].decode(self.encoding).strip(),
                    "右左": byte_str[25:26].decode(self.encoding).strip(),
                    "内外": byte_str[26:27].decode(self.encoding).strip(),
                    "種別": byte_str[27:29].decode(self.encoding).strip(),
                    "条件": byte_str[29:31].decode(self.encoding).strip(),
                    "記号": byte_str[31:34].decode(self.encoding).strip(),
                    "重量": byte_str[34:35].decode(self.encoding).strip(),
                    "グレード": byte_str[35:36].decode(self.encoding).strip(),
                    "レース名": byte_str[36:86].decode(self.encoding).strip(),
                    "回数": byte_str[86:94].decode(self.encoding).strip(),
                    "頭数": byte_str[94:96].decode(self.encoding).strip(),
                    "コース": byte_str[96:97].decode(self.encoding).strip(),
                    "開催区分": byte_str[97:98].decode(self.encoding).strip(),
                    "レース名短縮": byte_str[98:106].decode(self.encoding).strip(),
                    "レース名９文字": byte_str[106:124].decode(self.encoding).strip(),
                    "データ区分": byte_str[124:125].decode(self.encoding).strip(),
                    "１着賞金": byte_str[125:130].decode(self.encoding).strip(),
                    "２着賞金": byte_str[130:135].decode(self.encoding).strip(),
                    "３着賞金": byte_str[135:140].decode(self.encoding).strip(),
                    "４着賞金": byte_str[140:145].decode(self.encoding).strip(),
                    "５着賞金": byte_str[145:150].decode(self.encoding).strip(),
                    "１着算入賞金": byte_str[150:155].decode(self.encoding).strip(),
                    "２着算入賞金": byte_str[155:160].decode(self.encoding).strip(),
                    "馬券発売フラグ": byte_str[160:176].decode(self.encoding).strip(),
                    "WIN5フラグ": byte_str[176:177].decode(self.encoding).strip(),
                    "予備": byte_str[177:182].decode(self.encoding).strip(),
                }
                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                # データリストに行データを追加
                data_list.append(data)
        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_cza(self, file_path):
        """
        JRDBのCZAファイルを読み込み、データフレームに変換する関数。

        Parameters:
        - file_path (str): 読み込むCZAファイルのパス。

        Returns:
        - pd.DataFrame: CZAファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []

        # ファイルを開く（エンコーディングは仕様に合わせて設定）
        with open(file_path, "r", encoding=self.encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(self.encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "調教師コード": byte_str[0:5].decode(self.encoding).strip(),
                    "登録抹消フラグ": byte_str[5:6].decode(self.encoding).strip(),
                    "登録抹消年月日": byte_str[6:14].decode(self.encoding).strip(),
                    "調教師名": byte_str[14:26].decode(self.encoding).strip(),
                    "調教師カナ": byte_str[26:56].decode(self.encoding).strip(),
                    "調教師名略称": byte_str[56:62].decode(self.encoding).strip(),
                    "所属コード": byte_str[62:63].decode(self.encoding).strip(),
                    "所属地域名": byte_str[63:67].decode(self.encoding).strip(),
                    "生年月日": byte_str[67:75].decode(self.encoding).strip(),
                    "初免許年": byte_str[75:79].decode(self.encoding).strip(),
                    "調教師コメント": byte_str[79:119].decode(self.encoding).strip(),
                    "コメント入力年月日": byte_str[119:127].decode(self.encoding).strip(),
                    "本年リーディング": byte_str[127:130].decode(self.encoding).strip(),
                    "本年平地成績": byte_str[130:142].decode(self.encoding).strip(),
                    "本年障害成績": byte_str[142:154].decode(self.encoding).strip(),
                    "本年特別勝数": byte_str[154:157].decode(self.encoding).strip(),
                    "本年重賞勝数": byte_str[157:160].decode(self.encoding).strip(),
                    "昨年リーディング": byte_str[160:163].decode(self.encoding).strip(),
                    "昨年平地成績": byte_str[163:175].decode(self.encoding).strip(),
                    "昨年障害成績": byte_str[175:187].decode(self.encoding).strip(),
                    "昨年特別勝数": byte_str[187:190].decode(self.encoding).strip(),
                    "昨年重賞勝数": byte_str[190:193].decode(self.encoding).strip(),
                    "通算平地成績": byte_str[193:213].decode(self.encoding).strip(),
                    "通算障害成績": byte_str[213:233].decode(self.encoding).strip(),
                    "データ年月日": byte_str[233:241].decode(self.encoding).strip(),
                    "予備": byte_str[241:270].decode(self.encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_joa(self, file_path):
        """
        JRDBのJOAファイルを読み込み、データフレームに変換する関数。

        Parameters:
        - file_path (str): 読み込むJOAファイルのパス。

        Returns:
        - pd.DataFrame: JOAファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []

        # ファイルを開く（エンコーディングは仕様に合わせて設定）
        with open(file_path, "r", encoding=self.encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(self.encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": byte_str[0:2].decode(self.encoding).strip(),
                    "年": byte_str[2:4].decode(self.encoding).strip(),
                    "回": byte_str[4:5].decode(self.encoding).strip(),
                    "日": hex_to_dec(byte_str[5:6].decode(self.encoding).strip()),
                    "Ｒ": byte_str[6:8].decode(self.encoding).strip(),
                    "馬番": byte_str[8:10].decode(self.encoding).strip(),
                    "血統登録番号": byte_str[10:18].decode(self.encoding).strip(),
                    "馬名": byte_str[18:54].decode(self.encoding).strip(),
                    "基準オッズ": byte_str[54:59].decode(self.encoding).strip(),
                    "基準複勝オッズ": byte_str[59:64].decode(self.encoding).strip(),
                    "CID調教素点": byte_str[64:69].decode(self.encoding).strip(),
                    "CID厩舎素点": byte_str[69:74].decode(self.encoding).strip(),
                    "CID素点": byte_str[74:79].decode(self.encoding).strip(),
                    "CID": byte_str[79:82].decode(self.encoding).strip(),
                    "LS指数": byte_str[82:87].decode(self.encoding).strip(),
                    "LS評価": byte_str[87:88].decode(self.encoding).strip(),
                    "EM": byte_str[88:89].decode(self.encoding).strip(),
                    "厩舎ＢＢ印": byte_str[89:90].decode(self.encoding).strip(),
                    "厩舎ＢＢ◎単勝回収率": byte_str[90:95].decode(self.encoding).strip(),
                    "厩舎ＢＢ◎連対率": byte_str[95:100].decode(self.encoding).strip(),
                    "騎手ＢＢ印": byte_str[100:101].decode(self.encoding).strip(),
                    "騎手ＢＢ◎単勝回収率": byte_str[101:106].decode(self.encoding).strip(),
                    "騎手ＢＢ◎連対率": byte_str[106:111].decode(self.encoding).strip(),
                    "予備": byte_str[111:114].decode(self.encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_kza(self, file_path):
        """
        JRDBのKZAファイルを読み込み、データフレームに変換する関数。

        Parameters:
        - file_path (str): 読み込むKZAファイルのパス。

        Returns:
        - pd.DataFrame: KZAファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []

        # ファイルを開く（エンコーディングは仕様に合わせて設定）
        with open(file_path, "r", encoding=self.encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(self.encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "騎手コード": byte_str[0:5].decode(self.encoding).strip(),
                    "登録抹消フラグ": byte_str[5:6].decode(self.encoding).strip(),
                    "登録抹消年月日": byte_str[6:14].decode(self.encoding).strip(),
                    "騎手名": byte_str[14:26].decode(self.encoding).strip(),
                    "騎手カナ": byte_str[26:56].decode(self.encoding).strip(),
                    "騎手名略称": byte_str[56:62].decode(self.encoding).strip(),
                    "所属コード": byte_str[62:63].decode(self.encoding).strip(),
                    "所属地域名": byte_str[63:67].decode(self.encoding).strip(),
                    "生年月日": byte_str[67:75].decode(self.encoding).strip(),
                    "初免許年": byte_str[75:79].decode(self.encoding).strip(),
                    "見習い区分": byte_str[79:80].decode(self.encoding).strip(),
                    "所属厩舎": byte_str[80:85].decode(self.encoding).strip(),
                    "騎手コメント": byte_str[85:125].decode(self.encoding).strip(),
                    "コメント入力年月日": byte_str[125:133].decode(self.encoding).strip(),
                    "本年リーディング": byte_str[133:136].decode(self.encoding).strip(),
                    "本年平地成績": byte_str[136:148].decode(self.encoding).strip(),
                    "本年障害成績": byte_str[148:160].decode(self.encoding).strip(),
                    "本年特別勝数": byte_str[160:163].decode(self.encoding).strip(),
                    "本年重賞勝数": byte_str[163:166].decode(self.encoding).strip(),
                    "昨年リーディング": byte_str[166:169].decode(self.encoding).strip(),
                    "昨年平地成績": byte_str[169:181].decode(self.encoding).strip(),
                    "昨年障害成績": byte_str[181:193].decode(self.encoding).strip(),
                    "昨年特別勝数": byte_str[193:196].decode(self.encoding).strip(),
                    "昨年重賞勝数": byte_str[196:199].decode(self.encoding).strip(),
                    "通算平地成績": byte_str[199:219].decode(self.encoding).strip(),
                    "通算障害成績": byte_str[219:239].decode(self.encoding).strip(),
                    "データ年月日": byte_str[239:247].decode(self.encoding).strip(),
                    "予備": byte_str[247:270].decode(self.encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def save_to_csv(self, File_type, base_input_directory, output_directory):
        """
        指定されたディレクトリ内のファイルを読み込み、CSVに変換して保存する。

        Parameters:
        - base_input_directory (str): 読み込むファイルが格納されたディレクトリ。
        - output_directory (str): CSVファイルを保存するディレクトリ。
        """
        # 入力と出力のディレクトリパス
        base_input_directory_path = Path(base_input_directory)
        output_directory_path = Path(output_directory)
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
                    df = self.read_and_convert(file_path)
                    all_dfs.append(df)

            # すべてのDataFrameを結合
            final_df = pd.concat(all_dfs, ignore_index=True)
            # 出力ディレクトリが存在しない場合は作成
            ensure_directory_exists(output_directory_path)
            # 結合されたDataFrameをCSVとして保存
            final_df.to_csv(output_directory_path / f"{File_type}_{year}.csv", encoding="utf-8", index=False)
        return None
