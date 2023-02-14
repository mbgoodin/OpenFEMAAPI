import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import time
from fastparquet import ParquetFile
import datetime
import requests
import math
from time import sleep
import numpy as np


def makeTheCall(url):
    def callMetadata(url):
        add_on = '&$filter=declarationDate%20ge%20%272019-12-01%27&$inlinecount=allpages&$select=id&$top=1'
        print("URL:", url + add_on)
        request = requests.get(url + add_on)
        print(request.status_code)
        results = request.json()
        try:
            metadata = results['metadata']
            count = metadata['count']
            print(f'Status Code: {request.status_code}')
            print(f"Number of Records in dataset: {count}")
            return count
        except:
            print("Nope")

    def heavy_lifting(url, i, loop_size):
        decdatefilter = '&$filter=declarationDate%20ge%20%272019-12-01%27'
        url_var = "&$skip=" + str(i) + "&$top=" + str(loop_size)
        request = requests.get(url + decdatefilter + url_var)
        print(f'Status Code: {request.status_code}')
        return request

    ###########run the code###############
    now = datetime.datetime.now()
    count = callMetadata(url)
    # start setting up loop
    loop_size = 50_000  # number of records per loop
    num_loops = math.ceil(count / loop_size)  # loops that will be performed to iterate
    print(("-" * 5), f"This will take {num_loops} iterations", str("-" * 5))
    try:
        top = str(loop_size)
        i = 0
        bigDf = pd.DataFrame()
        for x in range(num_loops):
            print(f'starting i: {i}')
            r = heavy_lifting(url, i, top)  # actual request
            if str(r.status_code) != "200":
                print(f"Status code is {r.status_code}, sleeping for 10 and trying again.")
                sleep(10)
                r = heavy_lifting(url, i, top)  # actual request
            resultsJson = r.json()
            jsonKeys = [x for x in resultsJson][1]  # second key is always the data but different value
            parsedData = resultsJson[jsonKeys]  # parse out data
            parsedDf = pd.DataFrame.from_records(parsedData)  # create a df from the data
            bigDf = pd.concat([bigDf, parsedDf])  # concat data to bigDf

            i += loop_size
            sleep(1)
        bigDf['dbRefreshDate'] = str(now)
        bigDf['declarationDate'] = pd.to_datetime(bigDf['declarationDate']).dt.tz_localize(None)
        bigDf['year'] = bigDf['declarationDate'].dt.year
        return bigDf
    except Exception as e:
        print(e)
        print("Unable to complete an iteration.")
        exit()


def addCategoryColumn(df):
    print("Adding Category Column")
    #col CC
    # add check to ensure integer
    col1 = 'ownRent'
    ppfvl = 'ppfvl'
    rpfvl = 'rpfvl'
    renter = df[col1] == 'Renter'
    owner = df[col1] == 'Owner'
    conditions = [
        (renter) & (df[ppfvl] > 7_500),
        (renter) & (df[ppfvl] >= 3_500) & (df[ppfvl] <= 7_500),
        (renter) & (df[ppfvl] >= 2_000) & (df[ppfvl] < 3_500),
        (renter) & (df[ppfvl] >= 1_000) & (df[ppfvl] < 2_000),
        (renter) & (df[ppfvl] < 1_000),
        (owner) & ((df[rpfvl] > 28_800) | (df[ppfvl] > 9_000)),
        (owner) & (((df[rpfvl] >= 15_000) & (df[rpfvl] <= 28_800)) |
                   ((df[ppfvl] >= 5_000) & (df[ppfvl] <= 9_000))),
        (owner) & (((df[rpfvl] >= 8_000) & (df[rpfvl] < 15_000)) |
                   ((df[ppfvl] >= 3_500) & (df[ppfvl] < 5_000))),
        (owner) & ((df[rpfvl] >= 3_000) & (df[rpfvl] < 8_000) |
                   (df[ppfvl] >= 2_500) & (df[ppfvl] < 3_500)),
        (owner) & ((df[rpfvl] < 3_000) | (df[ppfvl] < 2_500))
                   ]
    choices = ['Severe', 'Major-High', 'Major-Low', 'Minor-High', 'Minor-Low', 'Severe',
               'Major-High', "Major-Low", 'Minor-High', 'Minor-Low']
    df['Categories'] = np.select(conditions, choices, default=np.nan)
    return df


def RealPropertyDamageCategories(df):
    print("Adding Real Property Damage Categories")
    # col CF
    # add check to ensure integer
    col1 = 'ownRent'
    ppfvl = 'ppfvl'
    rpfvl = 'rpfvl'
    renter = df[col1] == 'Renter'
    owner = df[col1] == 'Owner'
    conditions = [
        (renter) & (df[ppfvl] > 7_500),
        (renter) & (df[ppfvl] >= 3_500) & (df[ppfvl] <= 7_500),
        (renter) & (df[ppfvl] >= 2_000) & (df[ppfvl] < 3_500),
        (renter) & (df[ppfvl] >= 1_000) & (df[ppfvl] < 2_000),
        (renter) & (df[ppfvl] < 1_000),
        (owner) & (df[rpfvl] > 28_800),
        (owner) & ((df[rpfvl] >= 15_000) | (df[rpfvl] <= 28_800)),
        (owner) & ((df[rpfvl] >= 8_000) | (df[rpfvl] < 15_000)),
        (owner) & ((df[rpfvl] >= 3_000) | (df[rpfvl] < 8_000)),
        (owner) & (df[rpfvl] < 3_000)
    ]
    choices = ['Severe', 'Major-High', 'Major-Low', 'Minor-High', 'Minor-Low', 'Severe',
               'Major-High', "Major-Low", 'Minor-High', 'Minor-Low']
    df['Real Property Damage Categories'] = np.select(conditions, choices, default=np.nan)
    return df

def addTotalFVLCol(df):
    print("Adding Total FVL")
    # col CD
    # add check to ensure integer
    df['TotalFVL'] = df['rpfvl'] + df['ppfvl']
    return df

def addTotalFVLEval(df):
    print("Adding Total FVL Eval")
    # col CE
    # add check to ensure integer
    df['TotalFVL>0'] = np.where(df['TotalFVL'] > 0, True, False)
    return df

def totalEstDmg(df):
    print("Adding Total Est Damage")
    #column H
    df['Total Estimated Damage'] = df['floodDamageAmount']
    return df

def totalAssistance(df):
    print("Adding Total Assistance")
    #col I
    df['Total Assistance'] = df['repairAmount'] + df['personalPropertyAmount']
    return df

def unmetNeeds(df):
    print("Adding Unmet Needs")
    #col J
    h3 = 'Total Estimated Damage'
    i3 = 'Total Assistance'
    conditions = [(df[h3]-df[i3]) < 0]
    choices = [0]
    default = (df[h3]-df[i3])
    df['Unmet Needs'] = np.select(conditions, choices, default=default)
    return df

def midEligibleUnit(df):
    print("Adding Mid Eligiblie Unit")
    #col K
    l3 = df['SF RPD Over 8k']
    m3 = df['SF RPD Over 3k']
    n3 = df['SF Flood Over 1ft']
    o3 = df['RU PP Over 2k']
    p3 = df['RU Flood Over 1ft']
    conditions = [(l3 + m3 + n3 + o3 + p3) >= 1]
    choices = [1]
    default = 0
    df['Unmet Needs'] = np.select(conditions, choices, default=default)
    return df

def sfRPDOver8k(df):
    print("Adding SFRPD Over 8k")
    #col L
    con1 = df['ownRent'] == 'Owner'
    con2 = df['rpfvl'] > 8000
    conditions = [(con1 & con2)]
    choices = [1]
    default = 0
    df['SF RPD Over 8k'] = np.select(conditions, choices, default=default)
    return df

def sfPPDover3k(df):
    print("Adding SFPPD Over 3k")
    #col M
    con1 = df['ownRent'] == 'Owner'
    con2 = df['ppfvl'] > 3500
    conditions = [(con1 & con2)]
    choices = [1]
    default = 0
    df['SF RPD Over 3k'] = np.select(conditions, choices, default=default)
    return df

def floodOver1ft(df):
    print("Adding flood Over 1ft")
    #col N
    con1 = df['ownRent'] == 'Owner'
    con2 = df['floodDamage'] == 1
    conditions = [(con1 & con2)]
    choices = [1]
    default = 0
    df['SF Flood Over 1ft'] = np.select(conditions, choices, default=default)
    return df

def ruPPover2k(df):
    print("Adding RUPP Over 2k")
    #col O
    con1 = df['ownRent'] == 'Renter'
    con2 = df['ppfvl'] >3500
    conditions = [(con1 & con2)]
    choices = [1]
    default = 0
    df['RU PP Over 2k'] = np.select(conditions, choices, default=default)
    return df

def ruFloodOver1ft(df):
    print("Adding RU Flood Over 1ft")
    #col P
    con1 = df['ownRent'] == 'Renter'
    con2 = df['floodDamage'] == 1
    conditions = [(con1 & con2)]
    choices = [1]
    default = 0
    df['RU Flood Over 1ft'] = np.select(conditions, choices, default=default)
    return df

def multipliedRPFVL2(df):
    #col CH
    #need SBA multiplier
    pass

def sbaMultipliedDamageCat(df):
    #col CI
    #need SBA multiplier
    pass

def sbaMultipliedTotalFVL(df):
    #col CJ
    #need SBA multiplier
    pass


def dropCols(df):
    print("Dropping columns.")
    colsToDrop = ['lastRefresh', 'id', 'ihpMax', 'haMax', 'onaMax']
    df = df.drop(columns=colsToDrop)
    return df


def formatDf(df):
    print("Formatting df.")
    df = df.applymap(str)
    return df


def convParquet(df):
    print("Converting df to parquet.")
    df.to_csv('IntermediateTable.csv', index=False)
    timestamp = datetime.datetime.now().strftime("%m%d%Y")
    table = pa.Table.from_pandas(df)
    name = 'IndividualsandHouseholdsProgramValidRegistrations'
    master = pq.write_table(table, f'{name}_Master.parquet')
    backup = pq.write_table(table, f'{name}{timestamp}.parquet')

if __name__ == "__main__":
    startTime = time.time()
    url = 'https://www.fema.gov/api/open/v1/IndividualsAndHouseholdsProgramValidRegistrations?'
    df = makeTheCall(url=url)
    df = addCategoryColumn(df)
    df = RealPropertyDamageCategories(df)
    df = addTotalFVLCol(df)
    df = addTotalFVLEval(df)
    df = totalEstDmg(df)
    df = totalAssistance(df)
    df = unmetNeeds(df)
    df = sfRPDOver8k(df)
    df = sfPPDover3k(df)
    df = floodOver1ft(df)
    df = ruPPover2k(df)
    df = ruFloodOver1ft(df)
    df = midEligibleUnit(df)
    df = formatDf(df)
    convParquet(df)
    print("Script complete.")
    executionTime = (time.time() - startTime)
    print(executionTime)

