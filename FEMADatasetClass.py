import pandas as pd
import femaDsFunctions


class FEMADataset:

    def getDataset(self, url):
        return femaDsFunctions.getData(url)
