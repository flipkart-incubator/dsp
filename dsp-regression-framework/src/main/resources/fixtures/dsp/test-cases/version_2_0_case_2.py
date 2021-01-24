import numpy as np
import pandas as pd
import statsmodels.api as sm

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#INPUT: regression_hive_table_input_test_dataframe
#OUTPUT: regression_hdfs_1_output_test_dataframe_case_2
X = regression_hive_table_input_test_dataframe["primarylistingppvs"]
y = regression_hive_table_input_test_dataframe["secondarylistingppvs"]
model = sm.OLS(y, X).fit()
model.summary()
predictions = model.predict(X)
regression_hdfs_1_output_test_dataframe_case_2 = predictions.to_frame()

#INPUT: regression_csv_in_memory_input_test_dataframe
#OUTPUT: regression_hdfs_2_output_test_dataframe_case_2
X = regression_csv_in_memory_input_test_dataframe["addtocartclicks"]
y = regression_csv_in_memory_input_test_dataframe["addtocartclicks"]
model = sm.OLS(y, X).fit()
model.summary()
predictions = model.predict(X)
regression_hdfs_2_output_test_dataframe_case_2 = predictions.to_frame()

#INPUT: regression_csv_map_reduce_input_test_dataframe
#OUTPUT: regression_hdfs_3_output_test_dataframe_case_2
X = regression_csv_map_reduce_input_test_dataframe["addtocartclicks"]
y = regression_csv_map_reduce_input_test_dataframe["addtocartclicks"]
model = sm.OLS(y, X).fit()
model.summary()
predictions = model.predict(X)
regression_hdfs_3_output_test_dataframe_case_2 = predictions.to_frame()