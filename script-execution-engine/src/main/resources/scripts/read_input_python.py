import pandas as pd
import glob
import os
import gc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

files = glob.glob('PATH*')

dfList = []
for f in files:
    try:
        df = pd.read_csv(f, header=HEADERS, sep='SEPARATOR', quotechar='\QUOTE_CHAR', na_values=NA_VALUES, encoding='ENCODING', dtype={DATE_TYPE})
        dfList.append(df)
        del df
        os.remove(f)
        gc.collect()
    except pd.errors.EmptyDataError:
        continue

globals()['DATAFRAME_NAME'] = pd.concat(dfList, ignore_index=True)
globals()['DATAFRAME_NAME'].name = 'DATAFRAME_NAME'
logger.info("DataFrame Generation completed for dataFrame: %s", globals()['DATAFRAME_NAME'].name)
