import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np  # np.nanを使用するために追加
import pandas as pd
from lib_func import detect_encoding, hex_to_dec
from tqdm import tqdm


class JRDBFileConverter:
    """
    JRDBの特定のファイルタイプ（BAC、CZAなど）をCSVに変換するクラス。

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
        elif self.file_type == "CZA":
            return self.read_and_convert_cza(file_path)
        elif self.file_type == "JOA":
            return self.read_and_convert_joa(file_path)
        elif self.file_type == "KZA":
            return self.read_and_convert_kza(file_path)
        elif self.file_type == "SED":
            return self.read_and_convert_sed(file_path)
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

        # TODO: ZEDは保留
        # elif self.file_type == "ZED":
        #     return self.read_and_convert_bac(file_path)
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
        # ファイルのエンコーディングを検出
        detected_encoding = detect_encoding(file_path)

        # ファイルのエンコーディングでテキストファイルを開く
        with open(file_path, "r", encoding=detected_encoding) as f:
            for line in f:
                # 各行をSHIFT_JISでエンコード
                byte_str = line.encode(detected_encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": byte_str[0:2].decode(detected_encoding).strip(),
                    "年": byte_str[2:4].decode(detected_encoding).strip(),
                    "回": byte_str[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(byte_str[5:6].decode(detected_encoding).strip()),
                    "Ｒ": byte_str[6:8].decode(detected_encoding).strip(),
                    "年月日": byte_str[8:16].decode(detected_encoding).strip(),
                    "発走時間": byte_str[16:20].decode(detected_encoding).strip(),
                    "距離": byte_str[20:24].decode(detected_encoding).strip(),
                    "芝ダ障害コード": byte_str[24:25].decode(detected_encoding).strip(),
                    "右左": byte_str[25:26].decode(detected_encoding).strip(),
                    "内外": byte_str[26:27].decode(detected_encoding).strip(),
                    "種別": byte_str[27:29].decode(detected_encoding).strip(),
                    "条件": byte_str[29:31].decode(detected_encoding).strip(),
                    "記号": byte_str[31:34].decode(detected_encoding).strip(),
                    "重量": byte_str[34:35].decode(detected_encoding).strip(),
                    "グレード": byte_str[35:36].decode(detected_encoding).strip(),
                    "レース名": byte_str[36:86].decode(detected_encoding).strip(),
                    "回数": byte_str[86:94].decode(detected_encoding).strip(),
                    "頭数": byte_str[94:96].decode(detected_encoding).strip(),
                    "コース": byte_str[96:97].decode(detected_encoding).strip(),
                    "開催区分": byte_str[97:98].decode(detected_encoding).strip(),
                    "レース名短縮": byte_str[98:106].decode(detected_encoding).strip(),
                    "レース名９文字": byte_str[106:124].decode(detected_encoding).strip(),
                    "データ区分": byte_str[124:125].decode(detected_encoding).strip(),
                    "１着賞金": byte_str[125:130].decode(detected_encoding).strip(),
                    "２着賞金": byte_str[130:135].decode(detected_encoding).strip(),
                    "３着賞金": byte_str[135:140].decode(detected_encoding).strip(),
                    "４着賞金": byte_str[140:145].decode(detected_encoding).strip(),
                    "５着賞金": byte_str[145:150].decode(detected_encoding).strip(),
                    "１着算入賞金": byte_str[150:155].decode(detected_encoding).strip(),
                    "２着算入賞金": byte_str[155:160].decode(detected_encoding).strip(),
                    "馬券発売フラグ": byte_str[160:176].decode(detected_encoding).strip(),
                    "WIN5フラグ": byte_str[176:177].decode(detected_encoding).strip(),
                    "予備": byte_str[177:182].decode(detected_encoding).strip(),
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
        # detected_encoding = detect_encoding(file_path)
        detected_encoding = "SHIFT_JIS"

        # ファイルのエンコーディングでテキストファイルを開く
        with open(file_path, "r", encoding=detected_encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(detected_encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": byte_str[0:2].decode(detected_encoding).strip(),
                    "年": byte_str[2:4].decode(detected_encoding).strip(),
                    "回": byte_str[4:5].decode(detected_encoding).strip(),
                    "日": byte_str[5:6].decode(detected_encoding).strip(),
                    "Ｒ": byte_str[6:8].decode(detected_encoding).strip(),
                    "馬番": byte_str[8:10].decode(detected_encoding).strip(),
                    "曜日": byte_str[10:12].decode(detected_encoding).strip(),
                    "調教年月日": byte_str[12:20].decode(detected_encoding).strip(),
                    "回数": byte_str[20:21].decode(detected_encoding).strip(),
                    "調教コースコード": byte_str[21:23].decode(detected_encoding).strip(),
                    "追切種類": byte_str[23:24].decode(detected_encoding).strip(),
                    "追い状態": byte_str[24:26].decode(detected_encoding).strip(),
                    "乗り役": byte_str[26:27].decode(detected_encoding).strip(),
                    "調教Ｆ": byte_str[27:28].decode(detected_encoding).strip(),
                    "テンＦ": byte_str[28:31].decode(detected_encoding).strip(),
                    "中間Ｆ": byte_str[31:34].decode(detected_encoding).strip(),
                    "終いＦ": byte_str[34:37].decode(detected_encoding).strip(),
                    "テンＦ指数": byte_str[37:40].decode(detected_encoding).strip(),
                    "中間Ｆ指数": byte_str[40:43].decode(detected_encoding).strip(),
                    "終いＦ指数": byte_str[43:46].decode(detected_encoding).strip(),
                    "追切指数": byte_str[46:49].decode(detected_encoding).strip(),
                    "併せ結果": byte_str[49:50].decode(detected_encoding).strip(),
                    "追切種類（併せ馬）": byte_str[50:51].decode(detected_encoding).strip(),
                    "年齢": byte_str[51:53].decode(detected_encoding).strip(),
                    "クラス": byte_str[53:55].decode(detected_encoding).strip(),
                    "予備": byte_str[55:62].decode(detected_encoding).strip(),
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
        # ファイルのエンコーディングを検出
        detected_encoding = detect_encoding(file_path)
        # ファイルのエンコーディングでテキストファイルを開く
        with open(file_path, "r", encoding=detected_encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(detected_encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "調教師コード": byte_str[0:5].decode(detected_encoding).strip(),
                    "登録抹消フラグ": byte_str[5:6].decode(detected_encoding).strip(),
                    "登録抹消年月日": byte_str[6:14].decode(detected_encoding).strip(),
                    "調教師名": byte_str[14:26].decode(detected_encoding).strip(),
                    "調教師カナ": byte_str[26:56].decode(detected_encoding).strip(),
                    "調教師名略称": byte_str[56:62].decode(detected_encoding).strip(),
                    "所属コード": byte_str[62:63].decode(detected_encoding).strip(),
                    "所属地域名": byte_str[63:67].decode(detected_encoding).strip(),
                    "生年月日": byte_str[67:75].decode(detected_encoding).strip(),
                    "初免許年": byte_str[75:79].decode(detected_encoding).strip(),
                    "調教師コメント": byte_str[79:119].decode(detected_encoding).strip(),
                    "コメント入力年月日": byte_str[119:127].decode(detected_encoding).strip(),
                    "本年リーディング": byte_str[127:130].decode(detected_encoding).strip(),
                    "本年平地成績": byte_str[130:142].decode(detected_encoding).strip(),
                    "本年障害成績": byte_str[142:154].decode(detected_encoding).strip(),
                    "本年特別勝数": byte_str[154:157].decode(detected_encoding).strip(),
                    "本年重賞勝数": byte_str[157:160].decode(detected_encoding).strip(),
                    "昨年リーディング": byte_str[160:163].decode(detected_encoding).strip(),
                    "昨年平地成績": byte_str[163:175].decode(detected_encoding).strip(),
                    "昨年障害成績": byte_str[175:187].decode(detected_encoding).strip(),
                    "昨年特別勝数": byte_str[187:190].decode(detected_encoding).strip(),
                    "昨年重賞勝数": byte_str[190:193].decode(detected_encoding).strip(),
                    "通算平地成績": byte_str[193:213].decode(detected_encoding).strip(),
                    "通算障害成績": byte_str[213:233].decode(detected_encoding).strip(),
                    "データ年月日": byte_str[233:241].decode(detected_encoding).strip(),
                    "予備": byte_str[241:270].decode(detected_encoding).strip(),
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
        detected_encoding = detect_encoding(file_path)
        # ファイルのエンコーディングでテキストファイルを開く
        with open(file_path, "r", encoding=detected_encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(detected_encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": byte_str[0:2].decode(detected_encoding).strip(),
                    "年": byte_str[2:4].decode(detected_encoding).strip(),
                    "回": byte_str[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(byte_str[5:6].decode(detected_encoding).strip()),
                    "Ｒ": byte_str[6:8].decode(detected_encoding).strip(),
                    "馬番": byte_str[8:10].decode(detected_encoding).strip(),
                    "血統登録番号": byte_str[10:18].decode(detected_encoding).strip(),
                    "馬名": byte_str[18:54].decode(detected_encoding).strip(),
                    "基準オッズ": byte_str[54:59].decode(detected_encoding).strip(),
                    "基準複勝オッズ": byte_str[59:64].decode(detected_encoding).strip(),
                    "CID調教素点": byte_str[64:69].decode(detected_encoding).strip(),
                    "CID厩舎素点": byte_str[69:74].decode(detected_encoding).strip(),
                    "CID素点": byte_str[74:79].decode(detected_encoding).strip(),
                    "CID": byte_str[79:82].decode(detected_encoding).strip(),
                    "LS指数": byte_str[82:87].decode(detected_encoding).strip(),
                    "LS評価": byte_str[87:88].decode(detected_encoding).strip(),
                    "EM": byte_str[88:89].decode(detected_encoding).strip(),
                    "厩舎ＢＢ印": byte_str[89:90].decode(detected_encoding).strip(),
                    "厩舎ＢＢ◎単勝回収率": byte_str[90:95].decode(detected_encoding).strip(),
                    "厩舎ＢＢ◎連対率": byte_str[95:100].decode(detected_encoding).strip(),
                    "騎手ＢＢ印": byte_str[100:101].decode(detected_encoding).strip(),
                    "騎手ＢＢ◎単勝回収率": byte_str[101:106].decode(detected_encoding).strip(),
                    "騎手ＢＢ◎連対率": byte_str[106:111].decode(detected_encoding).strip(),
                    "予備": byte_str[111:114].decode(detected_encoding).strip(),
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
        # ファイルのエンコーディングを検出
        detected_encoding = detect_encoding(file_path)
        # ファイルのエンコーディングでテキストファイルを開く
        with open(file_path, "r", encoding=detected_encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(detected_encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "騎手コード": byte_str[0:5].decode(detected_encoding).strip(),
                    "登録抹消フラグ": byte_str[5:6].decode(detected_encoding).strip(),
                    "登録抹消年月日": byte_str[6:14].decode(detected_encoding).strip(),
                    "騎手名": byte_str[14:26].decode(detected_encoding).strip(),
                    "騎手カナ": byte_str[26:56].decode(detected_encoding).strip(),
                    "騎手名略称": byte_str[56:62].decode(detected_encoding).strip(),
                    "所属コード": byte_str[62:63].decode(detected_encoding).strip(),
                    "所属地域名": byte_str[63:67].decode(detected_encoding).strip(),
                    "生年月日": byte_str[67:75].decode(detected_encoding).strip(),
                    "初免許年": byte_str[75:79].decode(detected_encoding).strip(),
                    "見習い区分": byte_str[79:80].decode(detected_encoding).strip(),
                    "所属厩舎": byte_str[80:85].decode(detected_encoding).strip(),
                    "騎手コメント": byte_str[85:125].decode(detected_encoding).strip(),
                    "コメント入力年月日": byte_str[125:133].decode(detected_encoding).strip(),
                    "本年リーディング": byte_str[133:136].decode(detected_encoding).strip(),
                    "本年平地成績": byte_str[136:148].decode(detected_encoding).strip(),
                    "本年障害成績": byte_str[148:160].decode(detected_encoding).strip(),
                    "本年特別勝数": byte_str[160:163].decode(detected_encoding).strip(),
                    "本年重賞勝数": byte_str[163:166].decode(detected_encoding).strip(),
                    "昨年リーディング": byte_str[166:169].decode(detected_encoding).strip(),
                    "昨年平地成績": byte_str[169:181].decode(detected_encoding).strip(),
                    "昨年障害成績": byte_str[181:193].decode(detected_encoding).strip(),
                    "昨年特別勝数": byte_str[193:196].decode(detected_encoding).strip(),
                    "昨年重賞勝数": byte_str[196:199].decode(detected_encoding).strip(),
                    "通算平地成績": byte_str[199:219].decode(detected_encoding).strip(),
                    "通算障害成績": byte_str[219:239].decode(detected_encoding).strip(),
                    "データ年月日": byte_str[239:247].decode(detected_encoding).strip(),
                    "予備": byte_str[247:270].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
        return pd.DataFrame(data_list)

    def read_and_convert_sed(self, file_path):
        """
        JRDBのSEDファイルを読み込み、データフレームに変換する関数。

        Parameters:
        - file_path (str): 読み込むSEDファイルのパス。

        Returns:
        - pd.DataFrame: SEDファイルの内容を格納したデータフレーム。
        """
        # 空のリストを作成して、各行のデータを格納する
        data_list = []
        # ファイルのエンコーディングを検出
        detected_encoding = detect_encoding(file_path)
        # ファイルのエンコーディングでテキストファイルを開く
        with open(file_path, "r", encoding=detected_encoding) as f:
            for line in f:
                # 各行をエンコード
                byte_str = line.encode(detected_encoding)
                # 各フィールドをバイト単位でスライスし、デコードしてデータを格納
                data = {
                    "場コード": byte_str[0:2].decode(detected_encoding).strip(),
                    "年": byte_str[2:4].decode(detected_encoding).strip(),
                    "回": byte_str[4:5].decode(detected_encoding).strip(),
                    "日": hex_to_dec(byte_str[5:6].decode(detected_encoding).strip()),
                    "Ｒ": byte_str[6:8].decode(detected_encoding).strip(),
                    "馬番": byte_str[8:10].decode(detected_encoding).strip(),
                    "血統登録番号": byte_str[10:18].decode(detected_encoding).strip(),
                    "年月日": byte_str[18:26].decode(detected_encoding).strip(),
                    "馬名": byte_str[26:62].decode(detected_encoding).strip(),
                    "距離": byte_str[62:66].decode(detected_encoding).strip(),
                    "芝ダ障害コード": byte_str[66:67].decode(detected_encoding).strip(),
                    "右左": byte_str[67:68].decode(detected_encoding).strip(),
                    "内外": byte_str[68:69].decode(detected_encoding).strip(),
                    "馬場状態": byte_str[69:71].decode(detected_encoding).strip(),
                    "種別": byte_str[71:73].decode(detected_encoding).strip(),
                    "条件": byte_str[73:75].decode(detected_encoding).strip(),
                    "記号": byte_str[75:78].decode(detected_encoding).strip(),
                    "重量": byte_str[78:79].decode(detected_encoding).strip(),
                    "グレード": byte_str[79:80].decode(detected_encoding).strip(),
                    "レース名": byte_str[80:130].decode(detected_encoding).strip(),
                    "頭数": byte_str[130:132].decode(detected_encoding).strip(),
                    "レース名略称": byte_str[132:140].decode(detected_encoding).strip(),
                    "着順": byte_str[140:142].decode(detected_encoding).strip(),
                    "異常区分": byte_str[142:143].decode(detected_encoding).strip(),
                    "タイム": byte_str[143:147].decode(detected_encoding).strip(),
                    "斤量": byte_str[147:150].decode(detected_encoding).strip(),
                    "騎手名": byte_str[150:162].decode(detected_encoding).strip(),
                    "調教師名": byte_str[162:174].decode(detected_encoding).strip(),
                    "確定単勝オッズ": byte_str[174:180].decode(detected_encoding).strip(),
                    "確定単勝人気順位": byte_str[180:182].decode(detected_encoding).strip(),
                    "ＩＤＭ": byte_str[182:185].decode(detected_encoding).strip(),
                    "素点": byte_str[185:188].decode(detected_encoding).strip(),
                    "馬場差": byte_str[188:191].decode(detected_encoding).strip(),
                    "ペース": byte_str[191:194].decode(detected_encoding).strip(),
                    "出遅": byte_str[194:197].decode(detected_encoding).strip(),
                    "位置取": byte_str[197:200].decode(detected_encoding).strip(),
                    "不利": byte_str[200:203].decode(detected_encoding).strip(),
                    "前不利": byte_str[203:206].decode(detected_encoding).strip(),
                    "中不利": byte_str[206:209].decode(detected_encoding).strip(),
                    "後不利": byte_str[209:212].decode(detected_encoding).strip(),
                    "レース": byte_str[212:215].decode(detected_encoding).strip(),
                    "コース取り": byte_str[215:216].decode(detected_encoding).strip(),
                    "上昇度コード": byte_str[216:217].decode(detected_encoding).strip(),
                    "クラスコード": byte_str[217:219].decode(detected_encoding).strip(),
                    "馬体コード": byte_str[219:220].decode(detected_encoding).strip(),
                    "気配コード": byte_str[220:221].decode(detected_encoding).strip(),
                    "レースペース": byte_str[221:222].decode(detected_encoding).strip(),
                    "馬ペース": byte_str[222:223].decode(detected_encoding).strip(),
                    "テン指数": byte_str[223:228].decode(detected_encoding).strip(),
                    "上がり指数": byte_str[228:233].decode(detected_encoding).strip(),
                    "ペース指数": byte_str[233:238].decode(detected_encoding).strip(),
                    "レースＰ指数": byte_str[238:243].decode(detected_encoding).strip(),
                    "1(2)着馬名": byte_str[243:255].decode(detected_encoding).strip(),
                    "1(2)着タイム差": byte_str[255:258].decode(detected_encoding).strip(),
                    "前３Ｆタイム": byte_str[258:261].decode(detected_encoding).strip(),
                    "後３Ｆタイム": byte_str[261:264].decode(detected_encoding).strip(),
                    "備考": byte_str[264:288].decode(detected_encoding).strip(),
                    "予備": byte_str[288:290].decode(detected_encoding).strip(),
                    "確定複勝オッズ下": byte_str[290:296].decode(detected_encoding).strip(),
                    "10時単勝オッズ": byte_str[296:302].decode(detected_encoding).strip(),
                    "10時複勝オッズ": byte_str[302:308].decode(detected_encoding).strip(),
                    "コーナー順位１": byte_str[308:310].decode(detected_encoding).strip(),
                    "コーナー順位２": byte_str[310:312].decode(detected_encoding).strip(),
                    "コーナー順位３": byte_str[312:314].decode(detected_encoding).strip(),
                    "コーナー順位４": byte_str[314:316].decode(detected_encoding).strip(),
                    "前３Ｆ先頭差": byte_str[316:319].decode(detected_encoding).strip(),
                    "後３Ｆ先頭差": byte_str[319:322].decode(detected_encoding).strip(),
                    "騎手コード": byte_str[322:327].decode(detected_encoding).strip(),
                    "調教師コード": byte_str[327:332].decode(detected_encoding).strip(),
                    "馬体重": byte_str[332:335].decode(detected_encoding).strip(),
                    "馬体重増減": byte_str[335:338].decode(detected_encoding).strip(),
                    "天候コード": byte_str[338:339].decode(detected_encoding).strip(),
                    "コース": byte_str[339:340].decode(detected_encoding).strip(),
                    "レース脚質": byte_str[340:341].decode(detected_encoding).strip(),
                    "単勝": byte_str[341:348].decode(detected_encoding).strip(),
                    "複勝": byte_str[348:355].decode(detected_encoding).strip(),
                    "本賞金": byte_str[355:360].decode(detected_encoding).strip(),
                    "収得賞金": byte_str[360:365].decode(detected_encoding).strip(),
                    "レースペース流れ": byte_str[365:367].decode(detected_encoding).strip(),
                    "馬ペース流れ": byte_str[367:369].decode(detected_encoding).strip(),
                    "４角コース取り": byte_str[369:370].decode(detected_encoding).strip(),
                    "発走時間": byte_str[370:374].decode(detected_encoding).strip(),
                    "改行": byte_str[374:376].decode(detected_encoding).strip(),
                }

                # スペースをNaNに置換
                for key, value in data.items():
                    if value == " ":
                        data[key] = np.nan

                # データリストに行データを追加
                data_list.append(data)

        # データリストからデータフレームを作成して返す
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
