"""這個模組提供了一個功能，可以新增、刪除和瀏覽資料庫中的資料。"""
import sys
tab = [] #存放資料的列表

def modify_database(data_list):
    """
    這函數能新增資料、刪除資料、瀏覽資料庫的資料

    :param data_list: 用於存放資料的串列容器
    :return: 依照選擇會返回 str 或 list
    """
    while True:
        inp = input("請選擇(1)新增資料(2)刪除資料(3)瀏覽資料(4)終止:")

        if inp == str(1):
            data = input("請輸入預計新增的資料:")
            tab.append(data)

        elif inp == str(2):
            print(tab)
            dele = input("請輸入預計刪除的資料:")
            if dele in data_list:
                data_list.remove(dele)
            else:
                print("資料不存在。")

        elif inp == str(3):
            print(tab)

        elif inp == str(4):
            sys.exit()

        else:
            print("請輸入正確選項，選項只有1~4")

modify_database(tab)
