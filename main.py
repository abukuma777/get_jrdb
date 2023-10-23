from dl_data import download_and_organize_jrdb_data, download_KZA_CZA_MZA_data
from JRDBTxtToCSV import JRDBFileConverter

download_folder = "/app/data/jrdb_txt"

# TODO：最後コメントを解除

# # ----------------------------------------
# # 1. JRDBから、DL->解凍->txtを保存
# # ----------------------------------------
# list_File_type = ["Ov", "Paci", "Sed", "Skb"]

# for File_type in list_File_type:
#     # jrdb_data_downloader.pyの関数を呼び出す
#     # Paci, Ov, Sed, SkbをDL
#     download_and_organize_jrdb_data(File_type, download_folder)

# # KZA, CZA, MZAをDLする
# download_KZA_CZA_MZA_data(download_folder)

# # ----------------------------------------
# # 2. txt(エンコードバラバラ)->csv(UTF-8)
# # ----------------------------------------

# # BAC
# File_type = "BAC"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")


# # CHA
# File_type = "CHA"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # CYB
# File_type = "CYB"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # CZA
# File_type = "CZA"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # JOA
# File_type = "JOA"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # KAB
# File_type = "KAB"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # KKA
# File_type = "KKA"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # KYI
# File_type = "KYI"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # KZA
# File_type = "KZA"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # MZA
# File_type = "MZA"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # OT
# File_type = "OT"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # OU
# File_type = "OU"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # OV
# File_type = "OV"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # OW
# File_type = "OW"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # OZ
# File_type = "OZ"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")


# # SED
# File_type = "SED"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # SKB
# File_type = "SKB"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # TODO: 保留
# # SRB
# File_type = "SRB"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # UKC
# File_type = "UKC"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")


# ZED
File_type = "ZED"
converter = JRDBFileConverter(File_type)
converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")

# # ZKB
# File_type = "ZKB"
# converter = JRDBFileConverter(File_type)
# converter.save_to_csv(File_type, f"/app/data/jrdb_txt/{File_type}", f"/app/data/jrdb_csv/{File_type}")
