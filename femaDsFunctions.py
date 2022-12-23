import requests
from time import sleep
import math
import pandas as pd

def getData(url):
        '''
        The 'getData' function uses the requests library to send a first request to capture the metadata from
        the dataset and then return the number of records.
        The function then loops through requests modifying the url to only return the records between 'i'
        and 'i + loop_size"
        Finally, the data is added to a dataframe and returned.
        '''


        def retry(func, retries=3):
            '''The 'retry' function serves as a wrapper to retry get requests and post requests as needed before
            ultimately returning None if error connecting to the API
            The Open FEMA API is prone to timeouts from requests that are too fast and this will force a 5 second
            pause if that happens'''
            def retry_wrapper(*args, **kwargs):
                attempts = 0
                while attempts < retries:
                    try:
                        return func(*args, **kwargs)
                    except requests.exceptions.RequestException as e:
                        print(e)
                        print("Sleeping")
                        sleep(5)
                        attempts += 1
            return retry_wrapper

        def metadata_request(url):
            '''
            This function sends a request for a single record to return the metadata and determine
            the number of records in the dataset.
            '''
            add_on = '?$inlinecount=allpages&$select=id&$top=1'
            request = requests.get(url + add_on)
            results = request.json()
            metadata = results['metadata']
            count = metadata['count']
            print(f'Status Code: {request.status_code}')
            print(f"Number of Records in dataset: {count}")
            return count

        @retry
        def heavy_lifting(url, i, loop_size):
            '''
            The 'heavy_lifting' function is the key to get requests while looping.
            It takes the base url and modify it to return the records between the looping paramaters
            '''
            url_var = "?$skip=" + str(i) + "&$top=" + str(loop_size)
            request = requests.get(url + url_var)
            print(f'Status Code: {request.status_code}')

            return request


        # call a single record to get metadata
        count = metadata_request(url)
        #start setting up loop
        loop_size = 15_000  # number of records per loop
        num_loops = math.ceil(count / loop_size) # loops that will be performed to iterate
        print(("-"*5), f"This will take {num_loops} iterations", str("-"*5))

        try:
            top = str(loop_size)
            i = 0
            bigDf = pd.DataFrame()
            for x in range(num_loops):
                print(f'starting records: {i} to {loop_size+i}')
                r = heavy_lifting(url, i, top) #actual request
                resultsJson = r.json()
                jsonKeys = [x for x in resultsJson][1] #second key is always the data but different value
                parsedData = resultsJson[jsonKeys] #parse out data
                bigDf = pd.concat([bigDf, pd.DataFrame.from_records(parsedData)]) #concat data to bigDf
                i += loop_size
                sleep(1)

        except:
            print(Exception)
            print("Unable to complete an iteration.")
            exit()

        return bigDf


