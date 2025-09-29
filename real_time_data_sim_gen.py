import asyncio
import os
import pandas as pd
import itertools
import datetime

DATA_PATH_LIST = os.listdir('./datas')
base_path = './datas/'

async def periodical_data_gen(data:pd.DataFrame,period=2):
    for item in itertools.cycle(data.itertuples()):
        yield item
        await asyncio.sleep(period)

async def supervisor(data: pd.DataFrame, table: list, period=2):
    batch_time = 0
    async for item in periodical_data_gen(data, period):
        if len(table) >= BATCH_SIZE:
            await asyncio.to_thread(my_write, table)
            batch_time += 1
            table.clear()  # 正确清空
        now = pd.to_datetime(datetime.datetime.now())
        table.append([now] + list(item[1:]))


def my_write(table:list):

    with open(base_path+'show1.csv','a') as f:
        temp_df = pd.DataFrame(table,columns=columns)
        temp_df.to_csv(f,index=False,header=True if os.path.getsize(base_path+'show1.csv')==0 
                                                                        else False,mode='a')

if __name__ == '__main__':
    global columns
    global table
    BATCH_SIZE = 10
    data = pd.read_excel(base_path+DATA_PATH_LIST[2],header=0,usecols="B:J",nrows=9)
    columns = ["timestamp"]+list(data.columns[:])
    table = []
    asyncio.run(supervisor(data,table))