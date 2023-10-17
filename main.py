from dl_data import download_and_organize_jrdb_data
from dl_KZA_CZA_MZA import download_jockey_data

# ここでFile_typeを指定
list_File_type = ["Ov", "Paci"]
download_folder = "/app/data/jrdb_txt"

for File_type in list_File_type:
    # jrdb_data_downloader.pyの関数を呼び出す
    download_and_organize_jrdb_data(File_type, download_folder)

# KZA, CZA, MZAのデータをDLする
download_jockey_data(download_folder)
