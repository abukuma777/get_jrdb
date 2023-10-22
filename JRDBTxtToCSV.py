import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import chardet
import numpy as np  # np.nanを使用するために追加
import pandas as pd
from lib_func import hex_to_dec
from tqdm import tqdm


class JRDBFileConverter:
    """
    JRDBの特定のファイルタイプ（BAC、CZAなど）をCSVに変換するクラス。
    基本的に、前処理はここでは行わないが、日列が16進数のためそれだけはここで変換しておく。

    Attributes:
    - file_type (str): 変換するファイルのタイプ（例：'BAC', 'CZA'）。
    """

    def __init__(self, file_type):
        """
        コンストラクタ。

        Parameters:
        - file_type (str): 変換するファイルのタイプ。
        """
        self.file_type = file_type

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
        elif self.file_type == "CHA":
            return self.read_and_convert_cha(file_path)
        elif self.file_type == "CYB":
            return self.read_and_convert_cyb(file_path)
        elif self.file_type == "CZA":
            return self.read_and_convert_cza(file_path)
        elif self.file_type == "JOA":
            return self.read_and_convert_joa(file_path)
        elif self.file_type == "KAB":
            return self.read_and_convert_kab(file_path)
        elif self.file_type == "KKA":
            return self.read_and_convert_kka(file_path)
        elif self.file_type == "KYI":
            return self.read_and_convert_kyi(file_path)
        elif self.file_type == "KZA":
            return self.read_and_convert_kza(file_path)
        elif self.file_type == "MZA":
            return self.read_and_convert_mza(file_path)
        elif self.file_type == "OT":
            return self.read_and_convert_ot(file_path)
        elif self.file_type == "OU":
            return self.read_and_convert_ou(file_path)
        elif self.file_type == "OV":
            return self.read_and_convert_ov(file_path)
        elif self.file_type == "OW":
            return self.read_and_convert_ow(file_path)
        elif self.file_type == "OZ":
            return self.read_and_convert_oz(file_path)
        elif self.file_type == "SED" or self.file_type == "ZED":
            return self.read_and_convert_sed_zed(file_path)
        elif self.file_type == "SKB" or self.file_type == "ZKB":
            return self.read_and_convert_skb_zkb(file_path)
        elif self.file_type == "SRB":
            return self.read_and_convert_srb(file_path)
        elif self.file_type == "UKC":
            return self.read_and_convert_ukc(file_path)
        else:
            raise ValueError("Unsupported file type")

    def detect_encoding(self, file_path):
        """
        指定されたテキストファイルのエンコーディングを検出します。

        Parameters:
        - file_path (str): エンコーディングを検出するテキストファイルのパス。

        Returns:
        - str: 検出されたエンコーディング（例：'SHIFT_JIS', 'UTF-8'）。
        """
        # TODO: KAB?
        if self.file_type == "CHA":
            detected_encoding = "SHIFT_JIS"
        else:
            with open(file_path, "rb") as f:
                result = chardet.detect(f.read())
            detected_encoding = result["encoding"]
        return detected_encoding

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
        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "年月日": line[8:16].decode(detected_encoding).strip(),
                    "発走時間": line[16:20].decode(detected_encoding).strip(),
                    "距離": line[20:24].decode(detected_encoding).strip(),
                    "芝ダ障害コード": line[24:25].decode(detected_encoding).strip(),
                    "右左": line[25:26].decode(detected_encoding).strip(),
                    "内外": line[26:27].decode(detected_encoding).strip(),
                    "種別": line[27:29].decode(detected_encoding).strip(),
                    "条件": line[29:31].decode(detected_encoding).strip(),
                    "記号": line[31:34].decode(detected_encoding).strip(),
                    "重量": line[34:35].decode(detected_encoding).strip(),
                    "グレード": line[35:36].decode(detected_encoding).strip(),
                    "レース名": line[36:86].decode(detected_encoding).strip(),
                    "回数": line[86:94].decode(detected_encoding).strip(),
                    "頭数": line[94:96].decode(detected_encoding).strip(),
                    "コース": line[96:97].decode(detected_encoding).strip(),
                    "開催区分": line[97:98].decode(detected_encoding).strip(),
                    "レース名短縮": line[98:106].decode(detected_encoding).strip(),
                    "レース名９文字": line[106:124].decode(detected_encoding).strip(),
                    "データ区分": line[124:125].decode(detected_encoding).strip(),
                    "１着賞金": line[125:130].decode(detected_encoding).strip(),
                    "２着賞金": line[130:135].decode(detected_encoding).strip(),
                    "３着賞金": line[135:140].decode(detected_encoding).strip(),
                    "４着賞金": line[140:145].decode(detected_encoding).strip(),
                    "５着賞金": line[145:150].decode(detected_encoding).strip(),
                    "１着算入賞金": line[150:155].decode(detected_encoding).strip(),
                    "２着算入賞金": line[155:160].decode(detected_encoding).strip(),
                    "馬券発売フラグ": line[160:176].decode(detected_encoding).strip(),
                    "WIN5フラグ": line[176:177].decode(detected_encoding).strip(),
                    "予備": line[177:182].decode(detected_encoding).strip(),
                }
                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                # データリストに行データを追加
                data_list.append(data)
        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_cha(self, file_path):
        """
        CHA ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むCHAファイルのパス。

        Returns:
        - pd.DataFrame: CHAファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []
        # ファイルのエンコーディングを検出
        # detected_encoding = "SHIFT_JIS"
        detected_encoding = self.detect_encoding(file_path)

        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "曜日": line[10:12].decode(detected_encoding).strip(),
                    "調教年月日": line[12:20].decode(detected_encoding).strip(),
                    "回数": line[20:21].decode(detected_encoding).strip(),
                    "調教コースコード": line[21:23].decode(detected_encoding).strip(),
                    "追切種類": line[23:24].decode(detected_encoding).strip(),
                    "追い状態": line[24:26].decode(detected_encoding).strip(),
                    "乗り役": line[26:27].decode(detected_encoding).strip(),
                    "調教Ｆ": line[27:28].decode(detected_encoding).strip(),
                    "テンＦ": line[28:31].decode(detected_encoding).strip(),
                    "中間Ｆ": line[31:34].decode(detected_encoding).strip(),
                    "終いＦ": line[34:37].decode(detected_encoding).strip(),
                    "テンＦ指数": line[37:40].decode(detected_encoding).strip(),
                    "中間Ｆ指数": line[40:43].decode(detected_encoding).strip(),
                    "終いＦ指数": line[43:46].decode(detected_encoding).strip(),
                    "追切指数": line[46:49].decode(detected_encoding).strip(),
                    "併せ結果": line[49:50].decode(detected_encoding).strip(),
                    "追切種類（併せ馬）": line[50:51].decode(detected_encoding).strip(),
                    "年齢": line[51:53].decode(detected_encoding).strip(),
                    "クラス": line[53:55].decode(detected_encoding).strip(),
                    "予備": line[55:62].decode(detected_encoding).strip(),
                }
                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                # データリストに行データを追加
                data_list.append(data)
        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_cyb(self, file_path):
        """
        CYB ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むCYBファイルのパス。

        Returns:
        - pd.DataFrame: CYBファイルの内容を格納したデータフレーム。
        """
        data_list = []
        detected_encoding = self.detect_encoding(file_path)

        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "調教タイプ": line[10:12].decode(detected_encoding).strip(),
                    "調教コース種別": line[12:13].decode(detected_encoding).strip(),
                    "坂": line[13:15].decode(detected_encoding).strip(),
                    "Ｗ": line[15:17].decode(detected_encoding).strip(),
                    "ダ": line[17:19].decode(detected_encoding).strip(),
                    "芝": line[19:21].decode(detected_encoding).strip(),
                    "プ": line[21:23].decode(detected_encoding).strip(),
                    "障": line[23:25].decode(detected_encoding).strip(),
                    "ポ": line[25:27].decode(detected_encoding).strip(),
                    "調教距離": line[27:28].decode(detected_encoding).strip(),
                    "調教重点": line[28:29].decode(detected_encoding).strip(),
                    "追切指数": line[29:32].decode(detected_encoding).strip(),
                    "仕上指数": line[32:35].decode(detected_encoding).strip(),
                    "調教量評価": line[35:36].decode(detected_encoding).strip(),
                    "仕上指数変化": line[36:37].decode(detected_encoding).strip(),
                    "調教コメント": line[37:77].decode(detected_encoding).strip(),
                    "コメント年月日": line[77:85].decode(detected_encoding).strip(),
                    "調教評価": line[85:86].decode(detected_encoding).strip(),
                    "一週前追切指数": line[86:89].decode(detected_encoding).strip(),
                    "一週前追切コース": line[89:91].decode(detected_encoding).strip(),
                    "予備": line[91:94].decode(detected_encoding).strip(),
                }
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                data_list.append(data)

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
        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)
        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "調教師コード": line[0:5].decode(detected_encoding).strip(),
                    "登録抹消フラグ": line[5:6].decode(detected_encoding).strip(),
                    "登録抹消年月日": line[6:14].decode(detected_encoding).strip(),
                    "調教師名": line[14:26].decode(detected_encoding).strip(),
                    "調教師カナ": line[26:56].decode(detected_encoding).strip(),
                    "調教師名略称": line[56:62].decode(detected_encoding).strip(),
                    "所属コード": line[62:63].decode(detected_encoding).strip(),
                    "所属地域名": line[63:67].decode(detected_encoding).strip(),
                    "生年月日": line[67:75].decode(detected_encoding).strip(),
                    "初免許年": line[75:79].decode(detected_encoding).strip(),
                    "調教師コメント": line[79:119].decode(detected_encoding).strip(),
                    "コメント入力年月日": line[119:127].decode(detected_encoding).strip(),
                    "本年リーディング": line[127:130].decode(detected_encoding).strip(),
                    "本年平地成績": line[130:142].decode(detected_encoding).strip(),
                    "本年障害成績": line[142:154].decode(detected_encoding).strip(),
                    "本年特別勝数": line[154:157].decode(detected_encoding).strip(),
                    "本年重賞勝数": line[157:160].decode(detected_encoding).strip(),
                    "昨年リーディング": line[160:163].decode(detected_encoding).strip(),
                    "昨年平地成績": line[163:175].decode(detected_encoding).strip(),
                    "昨年障害成績": line[175:187].decode(detected_encoding).strip(),
                    "昨年特別勝数": line[187:190].decode(detected_encoding).strip(),
                    "昨年重賞勝数": line[190:193].decode(detected_encoding).strip(),
                    "通算平地成績": line[193:213].decode(detected_encoding).strip(),
                    "通算障害成績": line[213:233].decode(detected_encoding).strip(),
                    "データ年月日": line[233:241].decode(detected_encoding).strip(),
                    "予備": line[241:270].decode(detected_encoding).strip(),
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
        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)
        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "血統登録番号": line[10:18].decode(detected_encoding).strip(),
                    "馬名": line[18:54].decode(detected_encoding).strip(),
                    "基準オッズ": line[54:59].decode(detected_encoding).strip(),
                    "基準複勝オッズ": line[59:64].decode(detected_encoding).strip(),
                    "CID調教素点": line[64:69].decode(detected_encoding).strip(),
                    "CID厩舎素点": line[69:74].decode(detected_encoding).strip(),
                    "CID素点": line[74:79].decode(detected_encoding).strip(),
                    "CID": line[79:82].decode(detected_encoding).strip(),
                    "LS指数": line[82:87].decode(detected_encoding).strip(),
                    "LS評価": line[87:88].decode(detected_encoding).strip(),
                    "EM": line[88:89].decode(detected_encoding).strip(),
                    "厩舎ＢＢ印": line[89:90].decode(detected_encoding).strip(),
                    "厩舎ＢＢ◎単勝回収率": line[90:95].decode(detected_encoding).strip(),
                    "厩舎ＢＢ◎連対率": line[95:100].decode(detected_encoding).strip(),
                    "騎手ＢＢ印": line[100:101].decode(detected_encoding).strip(),
                    "騎手ＢＢ◎単勝回収率": line[101:106].decode(detected_encoding).strip(),
                    "騎手ＢＢ◎連対率": line[106:111].decode(detected_encoding).strip(),
                    "予備": line[111:114].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_kab(self, file_path):
        """
        KAB ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むKABファイルのパス。

        Returns:
        - pd.DataFrame: KABファイルの内容を格納したデータフレーム。
        """
        data_list = []
        # detected_encoding = "SHIFT_JIS"
        detected_encoding = self.detect_encoding(file_path)

        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "年月日": line[6:14].decode(detected_encoding).strip(),
                    "開催区分": line[14:15].decode(detected_encoding).strip(),
                    "曜日": line[15:17].decode(detected_encoding).strip(),
                    "場名": line[17:21].decode(detected_encoding).strip(),
                    "天候コード": line[21:22].decode(detected_encoding).strip(),
                    "芝馬場状態コード": line[22:24].decode(detected_encoding).strip(),
                    "芝馬場状態内": line[24:25].decode(detected_encoding).strip(),
                    "芝馬場状態中": line[25:26].decode(detected_encoding).strip(),
                    "芝馬場状態外": line[26:27].decode(detected_encoding).strip(),
                    "芝馬場差": line[27:30].decode(detected_encoding).strip(),
                    "直線馬場差最内": line[30:32].decode(detected_encoding).strip(),
                    "直線馬場差内": line[32:34].decode(detected_encoding).strip(),
                    "直線馬場差中": line[34:36].decode(detected_encoding).strip(),
                    "直線馬場差外": line[36:38].decode(detected_encoding).strip(),
                    "直線馬場差大外": line[38:40].decode(detected_encoding).strip(),
                    "ダ馬場状態コード": line[40:42].decode(detected_encoding).strip(),
                    "ダ馬場状態内": line[42:43].decode(detected_encoding).strip(),
                    "ダ馬場状態中": line[43:44].decode(detected_encoding).strip(),
                    "ダ馬場状態外": line[44:45].decode(detected_encoding).strip(),
                    "ダ馬場差": line[45:48].decode(detected_encoding).strip(),
                    "データ区分": line[48:49].decode(detected_encoding).strip(),
                    "連続何日目": line[49:51].decode(detected_encoding).strip(),
                    "芝種類": line[51:52].decode(detected_encoding).strip(),
                    "草丈": line[52:56].decode(detected_encoding).strip(),
                    "転圧": line[56:57].decode(detected_encoding).strip(),
                    "凍結防止剤": line[57:58].decode(detected_encoding).strip(),
                    "中間降水量": line[58:63].decode(detected_encoding).strip(),
                    "予備": line[63:70].decode(detected_encoding).strip(),
                }
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                data_list.append(data)

        return pd.DataFrame(data_list)

    def read_and_convert_kka(self, file_path):
        """
        KKA ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むKKAファイルのパス。

        Returns:
        - pd.DataFrame: KKAファイルの内容を格納したデータフレーム。
        """
        data_list = []
        detected_encoding = self.detect_encoding(file_path)

        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "ＪＲＡ成績": line[10:22].decode(detected_encoding).strip(),
                    "交流成績": line[22:34].decode(detected_encoding).strip(),
                    "他成績": line[34:46].decode(detected_encoding).strip(),
                    "芝ダ障害別成績": line[46:58].decode(detected_encoding).strip(),
                    "芝ダ障害別距離成績": line[58:70].decode(detected_encoding).strip(),
                    "トラック距離成績": line[70:82].decode(detected_encoding).strip(),
                    "ローテ成績": line[82:94].decode(detected_encoding).strip(),
                    "回り成績": line[94:106].decode(detected_encoding).strip(),
                    "騎手成績": line[106:118].decode(detected_encoding).strip(),
                    "良成績": line[118:130].decode(detected_encoding).strip(),
                    "稍成績": line[130:142].decode(detected_encoding).strip(),
                    "重成績": line[142:154].decode(detected_encoding).strip(),
                    "Ｓペース成績": line[154:166].decode(detected_encoding).strip(),
                    "Ｍペース成績": line[166:178].decode(detected_encoding).strip(),
                    "Ｈペース成績": line[178:190].decode(detected_encoding).strip(),
                    "季節成績": line[190:202].decode(detected_encoding).strip(),
                    "枠成績": line[202:214].decode(detected_encoding).strip(),
                    "騎手距離成績": line[214:226].decode(detected_encoding).strip(),
                    "騎手トラック距離成績": line[226:238].decode(detected_encoding).strip(),
                    "騎手調教師別成績": line[238:250].decode(detected_encoding).strip(),
                    "騎手馬主別成績": line[250:262].decode(detected_encoding).strip(),
                    "騎手ブリンカ成績": line[262:274].decode(detected_encoding).strip(),
                    "調教師馬主別成績": line[274:286].decode(detected_encoding).strip(),
                    "父馬産駒芝連対率": line[286:289].decode(detected_encoding).strip(),
                    "父馬産駒ダ連対率": line[289:292].decode(detected_encoding).strip(),
                    "父馬産駒連対平均距離": line[292:296].decode(detected_encoding).strip(),
                    "母父馬産駒芝連対率": line[296:299].decode(detected_encoding).strip(),
                    "母父馬産駒ダ連対率": line[299:302].decode(detected_encoding).strip(),
                    "母父馬産駒連対平均距離": line[302:306].decode(detected_encoding).strip(),
                    "予備": line[306:322].decode(detected_encoding).strip(),
                }

                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                data_list.append(data)
        return pd.DataFrame(data_list)

    def read_and_convert_kyi(self, file_path):
        """
        KYI ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むKYIファイルのパス。

        Returns:
        - pd.DataFrame: KYIファイルの内容を格納したデータフレーム。
        """
        data_list = []
        detected_encoding = self.detect_encoding(file_path)

        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "血統登録番号": line[10:18].decode(detected_encoding).strip(),
                    "馬名": line[18:54].decode(detected_encoding).strip(),
                    "IDM": line[54:59].decode(detected_encoding).strip(),
                    "騎手指数": line[59:64].decode(detected_encoding).strip(),
                    "情報指数": line[64:69].decode(detected_encoding).strip(),
                    "予備1": line[69:74].decode(detected_encoding).strip(),
                    "予備2": line[74:79].decode(detected_encoding).strip(),
                    "予備3": line[79:84].decode(detected_encoding).strip(),
                    "総合指数": line[84:89].decode(detected_encoding).strip(),
                    "脚質": line[89:90].decode(detected_encoding).strip(),
                    "距離適性": line[90:91].decode(detected_encoding).strip(),
                    "上昇度": line[91:92].decode(detected_encoding).strip(),
                    "ローテーション": line[92:95].decode(detected_encoding).strip(),
                    "基準オッズ": line[95:100].decode(detected_encoding).strip(),
                    "基準人気順位": line[100:102].decode(detected_encoding).strip(),
                    "基準複勝オッズ": line[102:107].decode(detected_encoding).strip(),
                    "基準複勝人気順位": line[107:109].decode(detected_encoding).strip(),
                    "特定情報◎": line[109:112].decode(detected_encoding).strip(),
                    "特定情報○": line[112:115].decode(detected_encoding).strip(),
                    "特定情報▲": line[115:118].decode(detected_encoding).strip(),
                    "特定情報△": line[118:121].decode(detected_encoding).strip(),
                    "特定情報×": line[121:124].decode(detected_encoding).strip(),
                    "総合情報◎": line[124:127].decode(detected_encoding).strip(),
                    "総合情報○": line[127:130].decode(detected_encoding).strip(),
                    "総合情報▲": line[130:133].decode(detected_encoding).strip(),
                    "総合情報△": line[133:136].decode(detected_encoding).strip(),
                    "総合情報×": line[136:139].decode(detected_encoding).strip(),
                    "人気指数": line[139:144].decode(detected_encoding).strip(),
                    "調教指数": line[144:149].decode(detected_encoding).strip(),
                    "厩舎指数": line[149:154].decode(detected_encoding).strip(),
                    "調教矢印コード": line[154:155].decode(detected_encoding).strip(),
                    "厩舎評価コード": line[155:156].decode(detected_encoding).strip(),
                    "騎手期待連対率": line[156:160].decode(detected_encoding).strip(),
                    "激走指数": line[160:163].decode(detected_encoding).strip(),
                    "蹄コード": line[163:165].decode(detected_encoding).strip(),
                    "重適正コード": line[165:166].decode(detected_encoding).strip(),
                    "クラスコード": line[166:168].decode(detected_encoding).strip(),
                    "予備4": line[168:170].decode(detected_encoding).strip(),
                    "ブリンカー": line[170:171].decode(detected_encoding).strip(),
                    "騎手名": line[171:183].decode(detected_encoding).strip(),
                    "負担重量": line[183:186].decode(detected_encoding).strip(),
                    "見習い区分": line[186:187].decode(detected_encoding).strip(),
                    "調教師名": line[187:199].decode(detected_encoding).strip(),
                    "調教師所属": line[199:203].decode(detected_encoding).strip(),
                    "前走1競走成績キー": line[203:219].decode(detected_encoding).strip(),
                    "前走2競走成績キー": line[219:235].decode(detected_encoding).strip(),
                    "前走3競走成績キー": line[235:251].decode(detected_encoding).strip(),
                    "前走4競走成績キー": line[251:267].decode(detected_encoding).strip(),
                    "前走5競走成績キー": line[267:283].decode(detected_encoding).strip(),
                    "前走1レースキー": line[283:291].decode(detected_encoding).strip(),
                    "前走2レースキー": line[291:299].decode(detected_encoding).strip(),
                    "前走3レースキー": line[299:307].decode(detected_encoding).strip(),
                    "前走4レースキー": line[307:315].decode(detected_encoding).strip(),
                    "前走5レースキー": line[315:323].decode(detected_encoding).strip(),
                    "枠番": line[323:324].decode(detected_encoding).strip(),
                    "予備5": line[324:326].decode(detected_encoding).strip(),
                    "総合印": line[326:327].decode(detected_encoding).strip(),
                    "IDM印": line[327:328].decode(detected_encoding).strip(),
                    "情報印": line[328:329].decode(detected_encoding).strip(),
                    "騎手印": line[329:330].decode(detected_encoding).strip(),
                    "厩舎印": line[330:331].decode(detected_encoding).strip(),
                    "調教印": line[331:332].decode(detected_encoding).strip(),
                    "激走印": line[332:333].decode(detected_encoding).strip(),
                    "芝適性コード": line[333:334].decode(detected_encoding).strip(),
                    "ダ適性コード": line[334:335].decode(detected_encoding).strip(),
                    "騎手コード": line[335:340].decode(detected_encoding).strip(),
                    "調教師コード": line[340:345].decode(detected_encoding).strip(),
                    "予備6": line[345:346].decode(detected_encoding).strip(),
                    "賞金情報_獲得賞金": line[346:352].decode(detected_encoding).strip(),
                    "賞金情報_収得賞金": line[352:357].decode(detected_encoding).strip(),
                    "条件クラス": line[357:358].decode(detected_encoding).strip(),
                    "テン指数": line[358:363].decode(detected_encoding).strip(),
                    "ペース指数": line[363:368].decode(detected_encoding).strip(),
                    "上がり指数": line[368:373].decode(detected_encoding).strip(),
                    "位置指数": line[373:378].decode(detected_encoding).strip(),
                    "ペース予想": line[378:379].decode(detected_encoding).strip(),
                    "道中順位": line[379:381].decode(detected_encoding).strip(),
                    "道中差": line[381:383].decode(detected_encoding).strip(),
                    "道中内外": line[383:384].decode(detected_encoding).strip(),
                    "後3F順位": line[384:386].decode(detected_encoding).strip(),
                    "後3F差": line[386:388].decode(detected_encoding).strip(),
                    "後3F内外": line[388:389].decode(detected_encoding).strip(),
                    "ゴール順位": line[389:391].decode(detected_encoding).strip(),
                    "ゴール差": line[391:393].decode(detected_encoding).strip(),
                    "ゴール内外": line[393:394].decode(detected_encoding).strip(),
                    "展開記号": line[394:395].decode(detected_encoding).strip(),
                    "距離適性２": line[395:396].decode(detected_encoding).strip(),
                    "枠確定馬体重": line[396:399].decode(detected_encoding).strip(),
                    "枠確定馬体重増減": line[399:402].decode(detected_encoding).strip(),
                    "取消フラグ": line[402:403].decode(detected_encoding).strip(),
                    "性別コード": line[403:404].decode(detected_encoding).strip(),
                    "馬主名": line[404:444].decode(detected_encoding).strip(),
                    "馬主会コード": line[444:446].decode(detected_encoding).strip(),
                    "馬記号コード": line[446:448].decode(detected_encoding).strip(),
                    "激走順位": line[448:450].decode(detected_encoding).strip(),
                    "LS指数順位": line[450:452].decode(detected_encoding).strip(),
                    "テン指数順位": line[452:454].decode(detected_encoding).strip(),
                    "ペース指数順位": line[454:456].decode(detected_encoding).strip(),
                    "上がり指数順位": line[456:458].decode(detected_encoding).strip(),
                    "位置指数順位": line[458:460].decode(detected_encoding).strip(),
                    "騎手期待単勝率": line[460:464].decode(detected_encoding).strip(),
                    "騎手期待３着内率": line[464:468].decode(detected_encoding).strip(),
                    "輸送区分": line[468:469].decode(detected_encoding).strip(),
                    "走法": line[469:477].decode(detected_encoding).strip(),
                    "体型": line[477:501].decode(detected_encoding).strip(),
                    "体型総合１": line[501:504].decode(detected_encoding).strip(),
                    "体型総合２": line[504:507].decode(detected_encoding).strip(),
                    "体型総合３": line[507:510].decode(detected_encoding).strip(),
                    "馬特記１": line[510:513].decode(detected_encoding).strip(),
                    "馬特記２": line[513:516].decode(detected_encoding).strip(),
                    "馬特記３": line[516:519].decode(detected_encoding).strip(),
                    "馬スタート指数": line[519:523].decode(detected_encoding).strip(),
                    "馬出遅率": line[523:527].decode(detected_encoding).strip(),
                    "参考前走": line[527:529].decode(detected_encoding).strip(),
                    "参考前走騎手コード": line[529:534].decode(detected_encoding).strip(),
                    "万券指数": line[534:537].decode(detected_encoding).strip(),
                    "万券印": line[537:538].decode(detected_encoding).strip(),
                    "降級フラグ": line[538:539].decode(detected_encoding).strip(),
                    "激走タイプ": line[539:541].decode(detected_encoding).strip(),
                    "休養理由分類コード": line[541:543].decode(detected_encoding).strip(),
                    "フラグ": line[543:559].decode(detected_encoding).strip(),
                    "入厩何走目": line[559:561].decode(detected_encoding).strip(),
                    "入厩年月日": line[561:569].decode(detected_encoding).strip(),
                    "入厩何日前": line[569:572].decode(detected_encoding).strip(),
                    "放牧先": line[572:622].decode(detected_encoding).strip(),
                    "放牧先ランク": line[622:623].decode(detected_encoding).strip(),
                    "厩舎ランク": line[623:624].decode(detected_encoding).strip(),
                    "予備7": line[624:1022].decode(detected_encoding).strip(),
                    "改行": line[1022:1024].decode(detected_encoding).strip(),
                }

                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                data_list.append(data)

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
        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)
        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "騎手コード": line[0:5].decode(detected_encoding).strip(),
                    "登録抹消フラグ": line[5:6].decode(detected_encoding).strip(),
                    "登録抹消年月日": line[6:14].decode(detected_encoding).strip(),
                    "騎手名": line[14:26].decode(detected_encoding).strip(),
                    "騎手カナ": line[26:56].decode(detected_encoding).strip(),
                    "騎手名略称": line[56:62].decode(detected_encoding).strip(),
                    "所属コード": line[62:63].decode(detected_encoding).strip(),
                    "所属地域名": line[63:67].decode(detected_encoding).strip(),
                    "生年月日": line[67:75].decode(detected_encoding).strip(),
                    "初免許年": line[75:79].decode(detected_encoding).strip(),
                    "見習い区分": line[79:80].decode(detected_encoding).strip(),
                    "所属厩舎": line[80:85].decode(detected_encoding).strip(),
                    "騎手コメント": line[85:125].decode(detected_encoding).strip(),
                    "コメント入力年月日": line[125:133].decode(detected_encoding).strip(),
                    "本年リーディング": line[133:136].decode(detected_encoding).strip(),
                    "本年平地成績": line[136:148].decode(detected_encoding).strip(),
                    "本年障害成績": line[148:160].decode(detected_encoding).strip(),
                    "本年特別勝数": line[160:163].decode(detected_encoding).strip(),
                    "本年重賞勝数": line[163:166].decode(detected_encoding).strip(),
                    "昨年リーディング": line[166:169].decode(detected_encoding).strip(),
                    "昨年平地成績": line[169:181].decode(detected_encoding).strip(),
                    "昨年障害成績": line[181:193].decode(detected_encoding).strip(),
                    "昨年特別勝数": line[193:196].decode(detected_encoding).strip(),
                    "昨年重賞勝数": line[196:199].decode(detected_encoding).strip(),
                    "通算平地成績": line[199:219].decode(detected_encoding).strip(),
                    "通算障害成績": line[219:239].decode(detected_encoding).strip(),
                    "データ年月日": line[239:247].decode(detected_encoding).strip(),
                    "予備": line[247:270].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_mza(self, file_path):
        """
        MZA ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むMZAファイルのパス。

        Returns:
        - pd.DataFrame: MZAファイルの内容を格納したデータフレーム。
        """
        data_list = []
        detected_encoding = self.detect_encoding(file_path)

        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "血統登録番号": line[0:8].decode(detected_encoding).strip(),
                    "予備": line[8:14].decode(detected_encoding).strip(),
                }
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan
                data_list.append(data)

        return pd.DataFrame(data_list)

    def read_and_convert_ot(self, file_path):
        """
        OT ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むOTファイルのパス。

        Returns:
        - pd.DataFrame: OTファイルの内容を格納したデータフレーム。
        """
        # データを格納するための空のリスト
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # ファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各行を指定されたエンコーディングでエンコード

                # レースキー情報をスライスして抽出
                race_key = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                }

                # 登録頭数をスライスして抽出
                registered_head_count = line[8:10].decode(detected_encoding).strip()

                # 3連複オッズをスライスして抽出（816項目、各6バイト）
                # インデックスは10から始まり、6バイトごとにスライス
                trifecta_odds = [line[i : i + 6].decode(detected_encoding).strip() for i in range(10, 4906, 6)]

                # 各項目をデータフレーム用の辞書にまとめる
                data = {
                    **race_key,
                    "登録頭数": registered_head_count,
                    "３連複オッズ": trifecta_odds,
                    "予備": line[4906:4910].decode(detected_encoding).strip(),
                }

                # 空白（" "）をNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_ou(self, file_path):
        """
        OU ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むOUファイルのパス。

        Returns:
        - pd.DataFrame: OUファイルの内容を格納したデータフレーム。
        """
        # データを格納するための空のリスト
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # ファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各行を指定されたエンコーディングでエンコード

                # レースキー情報をスライスして抽出
                race_key = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                }

                # 登録頭数をスライスして抽出
                registered_head_count = line[8:10].decode(detected_encoding).strip()

                # 馬単オッズをスライスして抽出（306項目、各6バイト）
                # インデックスは10から始まり、6バイトごとにスライス
                exacta_odds = [line[i : i + 6].decode(detected_encoding).strip() for i in range(10, 1846, 6)]

                # 各項目をデータフレーム用の辞書にまとめる
                data = {
                    **race_key,
                    "登録頭数": registered_head_count,
                    "馬単オッズ": exacta_odds,
                    "予備": line[1846:1854].decode(detected_encoding).strip(),
                }

                # 空白（" "）をNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    # OV
    def read_and_convert_ov(self, file_path):
        """
        OV ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むOVファイルのパス。

        Returns:
        - pd.DataFrame: OVファイルの内容を格納したデータフレーム。
        """
        # データを格納するための空のリスト
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # ファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各行を指定されたエンコーディングでエンコード

                # レースキー情報をスライスして抽出
                race_key = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                }

                # 登録頭数をスライスして抽出
                registered_head_count = line[8:10].decode(detected_encoding).strip()

                # ３連単オッズをスライスして抽出（4896項目、各7バイト）
                # インデックスは10から始まり、7バイトごとにスライス
                trifecta_odds = [line[i : i + 7].decode(detected_encoding).strip() for i in range(10, 34282, 7)]

                # 各項目をデータフレーム用の辞書にまとめる
                data = {
                    **race_key,
                    "登録頭数": registered_head_count,
                    "３連単オッズ": trifecta_odds,
                    "予備": line[34282:34286].decode(detected_encoding).strip(),
                }

                # 空白（" "）をNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    # OW
    def read_and_convert_ow(self, file_path):
        """
        OW ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むOWファイルのパス。

        Returns:
        - pd.DataFrame: OWファイルの内容を格納したデータフレーム。
        """
        # データを格納するための空のリスト
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # ファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各行を指定されたエンコーディングでエンコード

                # レースキー情報をスライスして抽出
                race_key = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                }

                # 登録頭数をスライスして抽出
                registered_head_count = line[8:10].decode(detected_encoding).strip()

                # ワイドオッズをスライスして抽出（153項目、各5バイト）
                # インデックスは10から始まり、5バイトごとにスライス
                wide_odds = [line[i : i + 5].decode(detected_encoding).strip() for i in range(10, 775, 5)]

                # 各項目をデータフレーム用の辞書にまとめる
                data = {
                    **race_key,
                    "登録頭数": registered_head_count,
                    "ワイドオッズ": wide_odds,
                    "予備": line[775:778].decode(detected_encoding).strip(),
                }

                # 空白（" "）をNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_oz(self, file_path):
        """
        OZ ファイルを読み込み、データフレームに変換する。

        Parameters:
        - file_path (str): 読み込むOZファイルのパス。

        Returns:
        - pd.DataFrame: OZファイルの内容を格納したデータフレーム。
        """
        # データを格納するための空のリスト
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # ファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各行を指定されたエンコーディングでエンコード

                # レースキー情報をスライスして抽出
                race_key = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                }

                # 登録頭数をスライスして抽出
                registered_head_count = line[8:10].decode(detected_encoding).strip()

                # 各種オッズをスライスして抽出
                win_odds = [line[i : i + 5].decode(detected_encoding).strip() for i in range(10, 100, 5)]
                place_odds = [line[i : i + 5].decode(detected_encoding).strip() for i in range(100, 190, 5)]
                quinella_odds = [line[i : i + 5].decode(detected_encoding).strip() for i in range(190, 955, 5)]

                # 各項目をデータフレーム用の辞書にまとめる
                data = {
                    **race_key,
                    "登録頭数": registered_head_count,
                    "単勝オッズ": win_odds,
                    "複勝オッズ": place_odds,
                    "連勝オッズ": quinella_odds,
                }

                # 空白（" "）をNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_sed_zed(self, file_path):
        """
        JRDBのSED, ZEDファイルを読み込み、データフレームに変換する関数。

        Parameters:
        - file_path (str): 読み込むSEDファイルのパス。

        Returns:
        - pd.DataFrame: SEDファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []
        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)
        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "血統登録番号": line[10:18].decode(detected_encoding).strip(),
                    "年月日": line[18:26].decode(detected_encoding).strip(),
                    "馬名": line[26:62].decode(detected_encoding).strip(),
                    "距離": line[62:66].decode(detected_encoding).strip(),
                    "芝ダ障害コード": line[66:67].decode(detected_encoding).strip(),
                    "右左": line[67:68].decode(detected_encoding).strip(),
                    "内外": line[68:69].decode(detected_encoding).strip(),
                    "馬場状態": line[69:71].decode(detected_encoding).strip(),
                    "種別": line[71:73].decode(detected_encoding).strip(),
                    "条件": line[73:75].decode(detected_encoding).strip(),
                    "記号": line[75:78].decode(detected_encoding).strip(),
                    "重量": line[78:79].decode(detected_encoding).strip(),
                    "グレード": line[79:80].decode(detected_encoding).strip(),
                    "レース名": line[80:130].decode(detected_encoding).strip(),
                    "頭数": line[130:132].decode(detected_encoding).strip(),
                    "レース名略称": line[132:140].decode(detected_encoding).strip(),
                    "着順": line[140:142].decode(detected_encoding).strip(),
                    "異常区分": line[142:143].decode(detected_encoding).strip(),
                    "タイム": line[143:147].decode(detected_encoding).strip(),
                    "斤量": line[147:150].decode(detected_encoding).strip(),
                    "騎手名": line[150:162].decode(detected_encoding).strip(),
                    "調教師名": line[162:174].decode(detected_encoding).strip(),
                    "確定単勝オッズ": line[174:180].decode(detected_encoding).strip(),
                    "確定単勝人気順位": line[180:182].decode(detected_encoding).strip(),
                    "ＩＤＭ": line[182:185].decode(detected_encoding).strip(),
                    "素点": line[185:188].decode(detected_encoding).strip(),
                    "馬場差": line[188:191].decode(detected_encoding).strip(),
                    "ペース": line[191:194].decode(detected_encoding).strip(),
                    "出遅": line[194:197].decode(detected_encoding).strip(),
                    "位置取": line[197:200].decode(detected_encoding).strip(),
                    "不利": line[200:203].decode(detected_encoding).strip(),
                    "前不利": line[203:206].decode(detected_encoding).strip(),
                    "中不利": line[206:209].decode(detected_encoding).strip(),
                    "後不利": line[209:212].decode(detected_encoding).strip(),
                    "レース": line[212:215].decode(detected_encoding).strip(),
                    "コース取り": line[215:216].decode(detected_encoding).strip(),
                    "上昇度コード": line[216:217].decode(detected_encoding).strip(),
                    "クラスコード": line[217:219].decode(detected_encoding).strip(),
                    "馬体コード": line[219:220].decode(detected_encoding).strip(),
                    "気配コード": line[220:221].decode(detected_encoding).strip(),
                    "レースペース": line[221:222].decode(detected_encoding).strip(),
                    "馬ペース": line[222:223].decode(detected_encoding).strip(),
                    "テン指数": line[223:228].decode(detected_encoding).strip(),
                    "上がり指数": line[228:233].decode(detected_encoding).strip(),
                    "ペース指数": line[233:238].decode(detected_encoding).strip(),
                    "レースＰ指数": line[238:243].decode(detected_encoding).strip(),
                    "1(2)着馬名": line[243:255].decode(detected_encoding).strip(),
                    "1(2)着タイム差": line[255:258].decode(detected_encoding).strip(),
                    "前３Ｆタイム": line[258:261].decode(detected_encoding).strip(),
                    "後３Ｆタイム": line[261:264].decode(detected_encoding).strip(),
                    "備考": line[264:288].decode(detected_encoding).strip(),
                    "予備": line[288:290].decode(detected_encoding).strip(),
                    "確定複勝オッズ下": line[290:296].decode(detected_encoding).strip(),
                    "10時単勝オッズ": line[296:302].decode(detected_encoding).strip(),
                    "10時複勝オッズ": line[302:308].decode(detected_encoding).strip(),
                    "コーナー順位１": line[308:310].decode(detected_encoding).strip(),
                    "コーナー順位２": line[310:312].decode(detected_encoding).strip(),
                    "コーナー順位３": line[312:314].decode(detected_encoding).strip(),
                    "コーナー順位４": line[314:316].decode(detected_encoding).strip(),
                    "前３Ｆ先頭差": line[316:319].decode(detected_encoding).strip(),
                    "後３Ｆ先頭差": line[319:322].decode(detected_encoding).strip(),
                    "騎手コード": line[322:327].decode(detected_encoding).strip(),
                    "調教師コード": line[327:332].decode(detected_encoding).strip(),
                    "馬体重": line[332:335].decode(detected_encoding).strip(),
                    "馬体重増減": line[335:338].decode(detected_encoding).strip(),
                    "天候コード": line[338:339].decode(detected_encoding).strip(),
                    "コース": line[339:340].decode(detected_encoding).strip(),
                    "レース脚質": line[340:341].decode(detected_encoding).strip(),
                    "単勝": line[341:348].decode(detected_encoding).strip(),
                    "複勝": line[348:355].decode(detected_encoding).strip(),
                    "本賞金": line[355:360].decode(detected_encoding).strip(),
                    "収得賞金": line[360:365].decode(detected_encoding).strip(),
                    "レースペース流れ": line[365:367].decode(detected_encoding).strip(),
                    "馬ペース流れ": line[367:369].decode(detected_encoding).strip(),
                    "４角コース取り": line[369:370].decode(detected_encoding).strip(),
                    "発走時間": line[370:374].decode(detected_encoding).strip(),
                    "改行": line[374:376].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    # SKB
    def read_and_convert_skb_zkb(self, file_path):
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "馬番": line[8:10].decode(detected_encoding).strip(),
                    "血統登録番号": line[10:18].decode(detected_encoding).strip(),
                    "年月日": line[18:26].decode(detected_encoding).strip(),
                    "特記コード": [line[i : i + 3].decode(detected_encoding).strip() for i in range(26, 44, 3)],
                    "馬具コード": [line[i : i + 3].decode(detected_encoding).strip() for i in range(44, 68, 3)],
                    "脚元コード_総合": line[68:71].decode(detected_encoding).strip(),
                    "脚元コード_左前": line[71:74].decode(detected_encoding).strip(),
                    "脚元コード_右前": line[74:77].decode(detected_encoding).strip(),
                    "脚元コード_左後": line[77:80].decode(detected_encoding).strip(),
                    "脚元コード_右後": line[80:83].decode(detected_encoding).strip(),
                    "パドックコメント": line[83:123].decode(detected_encoding).strip(),
                    "脚元コメント": line[123:163].decode(detected_encoding).strip(),
                    "馬具(その他)コメント": line[163:203].decode(detected_encoding).strip(),
                    "レースコメント": line[203:243].decode(detected_encoding).strip(),
                    "ハミ": line[243:246].decode(detected_encoding).strip(),
                    "バンテージ": line[246:249].decode(detected_encoding).strip(),
                    "蹄鉄": line[249:252].decode(detected_encoding).strip(),
                    "蹄状態": line[252:255].decode(detected_encoding).strip(),
                    "ソエ": line[255:258].decode(detected_encoding).strip(),
                    "骨瘤": line[258:261].decode(detected_encoding).strip(),
                    "予備": line[261:272].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        return pd.DataFrame(data_list)

    # SRB
    def read_and_convert_srb(self, file_path):
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "場コード": line[0:2].decode(detected_encoding).strip(),
                    "年": line[2:4].decode(detected_encoding).strip(),
                    "回": line[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(line[5:6].decode(detected_encoding).strip()),
                    "Ｒ": line[6:8].decode(detected_encoding).strip(),
                    "ハロンタイム": [line[i : i + 3].decode(detected_encoding).strip() for i in range(9, 63, 3)],
                    "１コーナー": line[63:127].decode(detected_encoding).strip(),
                    "２コーナー": line[127:191].decode(detected_encoding).strip(),
                    "３コーナー": line[191:255].decode(detected_encoding).strip(),
                    "４コーナー": line[255:319].decode(detected_encoding).strip(),
                    "ペースアップ位置": [line[i : i + 1].decode(detected_encoding).strip() for i in range(319, 321, 1)],
                    "１角": line[321:324].decode(detected_encoding).strip(),
                    "２角": line[324:327].decode(detected_encoding).strip(),
                    "向正": line[327:330].decode(detected_encoding).strip(),
                    "３角": line[330:333].decode(detected_encoding).strip(),
                    "４角": line[333:338].decode(detected_encoding).strip(),
                    "直線": line[338:343].decode(detected_encoding).strip(),
                    "レースコメント": line[343:843].decode(detected_encoding).strip(),
                    "予備": line[843:851].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        return pd.DataFrame(data_list)

    # UKC
    def read_and_convert_ukc(self, file_path):
        data_list = []

        # ファイルのエンコーディングを検出
        detected_encoding = self.detect_encoding(file_path)

        # バイナリモードでファイルを開く
        with open(file_path, "rb") as f:
            for line in f:
                data = {
                    "血統登録番号": line[0:8].decode(detected_encoding).strip(),
                    "馬名": line[8:44].decode(detected_encoding).strip(),
                    "性別コード": line[44:45].decode(detected_encoding).strip(),
                    "毛色コード": line[45:47].decode(detected_encoding).strip(),
                    "馬記号コード": line[47:49].decode(detected_encoding).strip(),
                    "父馬名": line[49:85].decode(detected_encoding).strip(),
                    "母馬名": line[85:121].decode(detected_encoding).strip(),
                    "母父馬名": line[121:157].decode(detected_encoding).strip(),
                    "生年月日": line[157:165].decode(detected_encoding).strip(),
                    "父馬生年": line[165:169].decode(detected_encoding).strip(),
                    "母馬生年": line[169:173].decode(detected_encoding).strip(),
                    "母父馬生年": line[173:177].decode(detected_encoding).strip(),
                    "馬主名": line[177:217].decode(detected_encoding).strip(),
                    "馬主会コード": line[217:219].decode(detected_encoding).strip(),
                    "生産者名": line[219:259].decode(detected_encoding).strip(),
                    "産地名": line[259:267].decode(detected_encoding).strip(),
                    "登録抹消フラグ": line[267:268].decode(detected_encoding).strip(),
                    "データ年月日": line[268:276].decode(detected_encoding).strip(),
                    "父系統コード": line[276:280].decode(detected_encoding).strip(),
                    "母父系統コード": line[280:284].decode(detected_encoding).strip(),
                    "予備": line[284:290].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        return pd.DataFrame(data_list)

    def process_files(self, input_directory_path, temp_dir):
        filenames = os.listdir(input_directory_path)
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_file = {
                executor.submit(self.read_and_convert, os.path.join(input_directory_path, filename)): filename
                for filename in filenames
                if filename.endswith(".txt")
            }
            for future in tqdm(as_completed(future_to_file), total=len(future_to_file)):
                filename = future_to_file[future]
                try:
                    df = future.result()
                    # Save each DataFrame to a temporary CSV file
                    temp_csv_path = os.path.join(temp_dir, f"{filename}.csv")
                    df.to_csv(temp_csv_path, index=False)
                except Exception as e:
                    print(f"Error occurred while processing {filename}: {e}")

    def save_to_csv(self, File_type, base_input_directory, output_directory):
        base_input_directory_path = Path(base_input_directory)
        output_directory_path = Path(output_directory)
        years = [f.name for f in base_input_directory_path.iterdir() if f.is_dir()]
        for year in tqdm(years):
            input_directory_path = base_input_directory_path / year
            temp_dir = os.path.join(output_directory, "temp")
            os.makedirs(temp_dir, exist_ok=True)
            self.process_files(input_directory_path, temp_dir)

            # Combine all temporary CSV files into one
            csv_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".csv")]
            combined_csv = pd.concat([pd.read_csv(f) for f in csv_files])
            combined_csv.to_csv(output_directory_path / f"{File_type}_{year}.csv", index=False, encoding="utf-8")

            # Remove temporary CSV files
            for f in csv_files:
                os.remove(f)
            os.rmdir(temp_dir)
