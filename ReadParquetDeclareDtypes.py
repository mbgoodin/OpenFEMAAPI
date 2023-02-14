import pandas as pd
import datetime
import pyarrow as pa
import pyarrow.parquet as pq

def readParquetFile():
    path = "C:\\Users\\52484\\Desktop\\ALL FEMA Stuff\\"
    filename = "IndividualsandHouseholdsProgramValidRegistrations_Master.parquet"
    df = pd.read_parquet(path + filename, engine='pyarrow')
    return df


def convCols(df):
    dateCols = ['declarationDate']
    numCols = ['year']
    boolCols = ['homeOwnersInsurance', 'floodInsurance', 'primaryResidence', 'ihpReferral', 'ihpEligible', 'haReferral',
                'haEligible', 'onaReferral', 'onaEligible', 'utilitiesOut', 'homeDamage', 'autoDamage',
                'emergencyNeeds',
                'accessFunctionalNeeds', 'sbaEligible', 'sbaApproved', 'inspnIssued', 'inspnReturned',
                'habitabilityRepairsRequired', 'destroyed', 'floodDamage', 'foundationDamage', 'roofDamage',
                'tsaEligible', 'tsaCheckedIn', 'rentalAssistanceEligible', 'repairAssistanceEligible',
                'replacementAssistanceEligible', 'personalPropertyEligible', 'ihpMax', 'haMax', 'onaMax']

    floatCols = ['ihpAmount', 'fipAmount', 'haAmount', 'onaAmount', 'rpfvl', 'ppfvl', 'waterLevel', 'floodDamageAmount',
                 'foundationDamageAmount', 'roofDamageAmount', 'rentalAssistanceAmount', 'repairAmount',
                 'replacementAmount', 'personalPropertyAmount', 'TotalFVL', 'Total Assistance', ]

    #df[dateCols] = pd.to_datetime(df[dateCols])
    df[numCols] = df[numCols].astype('int')
    df[boolCols] = df[boolCols].astype('bool')
    #df[floatCols] = df[floatCols].astype('float')

    print(df.dtypes)
    return df


def convParquet(df):
    timestamp = datetime.datetime.now().strftime("%m%d%Y")
    table = pa.Table.from_pandas(df)
    name = 'IndividualsandHouseholdsProgramValidRegistrations'
    master = pq.write_table(table, f'{name}_Master.parquet')
    backup = pq.write_table(table, f'{name}{timestamp}.parquet')


def main():
    print("Reading file.")
    df = readParquetFile()
    print("Converting columns.")
    df = convCols(df)
    print("Writing to parquet file.")
    convParquet(df)
    print("Complete!")

if __name__ == "__main__":
    main()