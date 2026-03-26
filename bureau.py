import pandas as pd


bureau = pd.read_csv(r'Data/bureau.csv')
bureau_balance = pd.read_csv(r'Data/bureau_balance.csv')

# Ordinal mapping (ONLY for true delinquency)
status_map = {
    '0': 0, '1': 1, '2': 2,
    '3': 3, '4': 4, '5': 5
}
bureau_balance['STATUS_ORD'] = bureau_balance['STATUS'].map(status_map)
bureau_balance['IS_DELINQUENT'] = bureau_balance['STATUS'].isin(['1','2','3','4','5']).astype(int)
bureau_balance['IS_CLOSED'] = (bureau_balance['STATUS'] == 'C').astype(int)
bureau_balance['IS_MISSING'] = (bureau_balance['STATUS'] == 'X').astype(int)
# Weight: recent months more important
bureau_balance['RECENCY_WEIGHT'] = 1 / (1 + abs(bureau_balance['MONTHS_BALANCE']))

# Weighted delinquency
bureau_balance['WEIGHTED_DELINQ'] = bureau_balance['IS_DELINQUENT'] * bureau_balance['RECENCY_WEIGHT']

bb_agg = bureau_balance.groupby('SK_ID_BUREAU').agg({
    'STATUS_ORD': ['mean', 'max'],
    'IS_DELINQUENT': ['mean', 'sum'],
    'IS_CLOSED': 'mean',
    'IS_MISSING': 'mean',
    'WEIGHTED_DELINQ': 'sum',
    'MONTHS_BALANCE': ['max', 'min']
})
bb_agg.columns = [
    'ORD_MEAN',
    'ORD_MAX',
    'DELINQ_RATIO',
    'DELINQ_COUNT',
    'CLOSED_RATIO',
    'MISSING_RATIO',
    'WEIGHTED_DELINQ_SUM',
    'RECENT_MONTH',
    'OLDEST_MONTH'
]

bb_agg = bb_agg.reset_index()
# merge the bureau data
bureau = bureau.merge(bb_agg, on='SK_ID_BUREAU', how='left')
bureau_agg = pd.DataFrame()
# agg per customer_id
bureau_agg['TOTAL_DEBT'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_SUM_DEBT'].sum()
bureau_agg['TOTAL_CREDIT'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_SUM'].sum()
bureau_agg['DEBT_RATIO'] = bureau_agg['TOTAL_DEBT'] / bureau_agg['TOTAL_CREDIT']

# Overdue behaviour
bureau_agg['TOTAL_OVERDUE'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_SUM_OVERDUE'].sum()
bureau_agg['MAX_OVERDUE'] = bureau.groupby('SK_ID_CURR')['AMT_CREDIT_MAX_OVERDUE'].max()

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
## TODO: Need to think Something better
bureau_agg["MAX_OVERDUE"] = bureau_agg["MAX_OVERDUE"].fillna(0)
bureau_agg = bureau_agg.reset_index()
## TODO: Temp Leaving Debt Ratio for now as it is less than 5% (1211/30000) but need to replace with missing value imputation technique with consideration on Debt Ratio as target variable.
print(bureau_agg.isnull().sum())
bureau_agg.to_csv(r"Data_processed/bureau.csv", index=False)