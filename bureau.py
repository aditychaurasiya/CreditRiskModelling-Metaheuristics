import pandas as pd
import numpy as np
import seaborn as sns

bureau = pd.read_csv(r'Data/bureau.csv')
bureau_agg = pd.DataFrame()
# agg per customer_id
bureau_agg['TOTAL_DEBT'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_SUM_DEBT'].sum()
bureau_agg['TOTAL_CREDIT'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_SUM'].sum()
bureau_agg['DEBT_RATIO'] = bureau_agg['TOTAL_DEBT'] / bureau_agg['TOTAL_CREDIT']

# Overdue behaviour
bureau_agg['TOTAL_OVERDUE'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_SUM_OVERDUE'].sum()
bureau_agg['MAX_OVERDUE'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_MAX_OVERDUE'].sum()

# Credit activity
bureau_agg['ACTIVE_LOANS'] = (
    bureau[bureau['CREDIT_ACTIVE'] == "Active"]
    .groupby('SK_ID_CURR')['CREDIT_ACTIVE']
    .count()
)

bureau_agg['CLOSED_LOANS'] = (
    bureau[bureau['CREDIT_ACTIVE'] != "Active"]
    .groupby('SK_ID_CURR')['CREDIT_ACTIVE']
    .count()
)
# fill active loans nan with 0 as these cust indicates no active loans
bureau_agg["ACTIVE_LOANS"] = bureau_agg["ACTIVE_LOANS"].fillna(0)
# Similarly Closed Loans
bureau_agg["CLOSED_LOANS"] = bureau_agg["CLOSED_LOANS"].fillna(0)
# print(bureau_agg.isnull().sum())

## TODO: Temp Leaving Debt Ratio for now as it is less than 5% (1211/30000) but need to replace with missing value imputation technique with consideration on Debt Ratio as target variable.
print(bureau_agg.isnull().sum())
print()