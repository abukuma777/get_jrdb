from dl_data import download_and_organize_jrdb_data
from dl_KZA_CZA_MZA import download_jockey_data
from JRDBTxtToCSV import JRDBFileConverter

# TODO：最後コメントを解除

# # ----------------------------------------
# # 1. JRDBから、DL->解凍->txtを保存
# # ----------------------------------------

# list_File_type = ["Ov", "Paci"]
# download_folder = "/app/data/jrdb_txt"

# for File_type in list_File_type:
#     # jrdb_data_downloader.pyの関数を呼び出す
#     # Paci, OvをDL
#     download_and_organize_jrdb_data(File_type, download_folder)

# # KZA, CZA, MZAをDLする
# download_jockey_data(download_folder)

# # ----------------------------------------
# # 2. txt(エンコードバラバラ)->csv(UTF-8)
# # ----------------------------------------

# BAC
bac_converter = JRDBFileConverter("BAC", "SHIFT_JIS")
bac_converter.save_to_csv("/app/data/jrdb_txt/BAC", "/app/data/jrdb_csv/BAC")
