# Import libraries
import pandas as pd

def main(dataset, dataset_to_join, join_key, join_columns):
    # If join columns isn't string, split to string
    join_conditions = [join_key]
    join_conditions.extend(join_columns)
    print(join_conditions)
    df = pd.merge(dataset, dataset_to_join[join_conditions], on=join_key, how='left')
    
    # TODO Extra if files get too big
    # Check if column exists in dataset or if values are in there already
    # If column doesn't exist - join
    # If column exists and values do for everything, return
    # If column exists and some values exist - take out rows without, join with dataframe, add back in, sort, return
    return df
