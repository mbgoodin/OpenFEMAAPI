import pandas as pd
from FEMADatasetClass import FEMADataset

'''
This is an example usage of the FEMADataset class.
OpenFEMA Data Sets info page: 'https://www.fema.gov/about/openfema/data-sets'

Get the api_endpoint from the specific dataset info page. Pay attention to the record count.
If you are trying to access a large dataset, the download will take a while. 

'''
# the url from the dataset info page labeled "API Endpoint"
api_endpoint = 'https://www.fema.gov/api/open/v2/FemaRegions'


testData = FEMADataset()
df = testData.getDataset(url=api_endpoint)

print(df.head(5))
print(df.dtypes)
print(type(df))