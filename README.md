# Open FEMA API

The documentation for this API is limited and the provided examples that I could find seemed to have a few headaches.

The approach that seemed the most succesful was to use the requests library and loop through requests.

Requirements
  1. pandas
  2. time
  3. math
  4. requests

There are 3 scripts:
  1. example.py - an example showing usage.
  2. FEMADatasetClass.py - the class
  3. femaDsFunctions.py - the file where the 'getData' function is stored.

The data will be returned as a pandas dataframe. Manipulate as needed.

The larger datasets that are over 5,000,000 records will take quite a while to run. ~15 minutes.

If I get a chance to continue development of this in a scalable fashion I will update this repo.

This is my first public repo and I expect to update this with more accepted practices at a later date. I also plan to refactor, time permitting.
