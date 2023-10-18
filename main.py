from dl_txt.dl_data import download_and_organize_jrdb_data
from dl_txt.dl_KZA_CZA_MZA import download_jockey_data
from output_csv.BAC_to_csv import BAC_save_to_csv

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
BAC_save_to_csv()
