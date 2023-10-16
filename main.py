from jrdb_data_downloader import download_and_organize_jrdb_data

# ここでFile_typeを指定
File_type = "Ov"
# File_type = "Paci"
download_folder = "/app/data/jrdb_txt"
# jrdb_data_downloader.pyの関数を呼び出す
download_and_organize_jrdb_data(File_type, download_folder)
