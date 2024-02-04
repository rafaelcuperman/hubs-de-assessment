from collections import defaultdict
from sqlalchemy import create_engine

import pandas as pd
import json
import argparse

pd.options.mode.chained_assignment = None


def extract(filename):
    """Extract phase

    Args:
        filename (str): .parquet file path

    Returns:
        pd.DataFrame: part data as dataframe
    """
    df = pd.read_parquet(filename)
    df.index.name = 'part_index'
    return df

def extract_hole_feature(hole, feature, subfeature):
    """Extract hole feature from json field

    Args:
        hole (dict): dict of hole features
        feature (str): feature to extract
        subfeature (str or None): subfeature to extract. Can be None

    Returns:
        str or number: extracted feature
    """
    try:
        if subfeature is not None:
            out = hole[feature][subfeature]
        else:
            out = hole[feature]
    except KeyError: 
        out = None
    return out

def extract_hole(hole):
    """Extract all the features of a hole

    Args:
        hole (dict): dict of hole features

    Returns:
        dict: hole features as a formatted dictionary with the form
            {'feature_1': value, ...}
    """
    d = {}
    
    d['center_x'] = extract_hole_feature(hole, 'center', 'x')
    d['center_y'] = extract_hole_feature(hole, 'center', 'y')
    d['center_z'] = extract_hole_feature(hole, 'center', 'z')
    
    d['direction_x'] = extract_hole_feature(hole, 'direction', 'x')
    d['direction_y'] = extract_hole_feature(hole, 'direction', 'y')
    d['direction_z'] = extract_hole_feature(hole, 'direction', 'z')
    
    d['end1_closed'] = extract_hole_feature(hole, 'end1', 'closed')
    d['end1_reachable'] = extract_hole_feature(hole, 'end1', 'reachable')
    
    d['end2_closed'] = extract_hole_feature(hole, 'end2', 'closed')
    d['end2_reachable'] = extract_hole_feature(hole, 'end2', 'reachable')
    
    d['facet_count'] = extract_hole_feature(hole, 'facet_count', None)
    d['length'] = extract_hole_feature(hole, 'length', None)
    d['radius'] = extract_hole_feature(hole, 'radius', None)

    return d

def extract_holes(holes):
    """Extract and combine features of all the holes of a part

    Args:
        holes (str): string of holes formatted as list of dicts

    Returns:
        dict: hole features as a formatted dictionary with the form
            {'feature_1': [value_1, value_2, ...], ...}
    """
    if pd.isnull(holes):
        dout = {}
    else: 
        holes = json.loads(holes)
        dout = defaultdict(list)
        for hole in holes: # Multiple holes as list
            d = extract_hole(hole)
            for key, value in d.items(): # Combine hole data in dictionary of features
                dout[key].append(value)
        dout = dict(dout)
    return dout

def create_holes_df(df):
    """Create dataframe of holes

    Args:
        df (pd.DataFrame): dataframe of parts
        
    Returns:
        pd.DataFrame: dataframe of holes
    """
    
    # Extract holes as dictionaries
    df_holes = df.apply(lambda row: extract_holes(row['holes']), axis = 1)
    
    # Convert dictionaries to dataframe
    df_holes = df_holes.apply(pd.Series)
    df_holes = df_holes.explode(list(df_holes.columns))
    
    # Remove empty rows (no holes for that part)
    df_holes.dropna(axis = 0, how = 'all', inplace = True)
    
    # Column names
    df_holes.reset_index(drop = False, inplace = True)
    df_holes.rename(columns = {'index': 'part_index'}, inplace = True)
    df_holes.index.name = 'hole_index'
    
    return df_holes

def has_warning(length, radius):
    """Create has_warning flag

    Args:
        length (float)
        radius (float)

    Returns:
        bool: has warning flag
    """
    return length > radius * 2 * 10

def has_error(length, radius):
    """Create has_error flag

    Args:
        length (float)
        radius (float)

    Returns:
        bool: has error flag
    """
    return length > radius * 2 * 40

def create_unreachable_df(df_holes):
    """Create unreachable dataframe

    Args:
        df_holes (pd.DataFrame): dataframe of holes

    Returns:
        pd.DataFrame: dataframe with unreachable hole flags
    """
    df_holes['has_unreachable_hole_warning'] = df_holes.apply(lambda row: has_warning(row['length'],
                                                                                      row['radius']),
                                                              axis = 1)
    df_holes['has_unreachable_hole_error'] = df_holes.apply(lambda row: has_error(row['length'],
                                                                                  row['radius']),
                                                            axis = 1)
    
    df_unreachable = df_holes[['has_unreachable_hole_warning', 'has_unreachable_hole_error']]
    df_holes = df_holes.drop(columns = ['has_unreachable_hole_warning', 'has_unreachable_hole_error'])
    
    return df_holes, df_unreachable
    
def transform(df):
    """Transform phase

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    df_holes = create_holes_df(df)
    df_holes, df_unreachable = create_unreachable_df(df_holes)
    
    return df_holes, df_unreachable
   
def load(df, df_holes, df_unreachable, write=False):
    """"
    The 3 dataframes must be saved:
        - df: dataframe of parts
        - df_holes: datagrame of holes of each part
        - df_unreachable: dataframe with unreachable flags of each hole
    Depending on the used database, this function must be modified.
    If write is True, the dataframes are loaded in the database
    If write is False, the dataframes are saved locally as csv files
    """
    
    # Example to write on local Postgres database
    if write:
        engine = create_engine('postgresql://root:root@localhost:5432/part_data')
        df.to_sql(name='parts', con=engine, if_exists='replace')
        df_holes.to_sql(name='holes', con=engine, if_exists='replace')
        df_unreachable.to_sql(name='unreachable_flags', con=engine, if_exists='replace')
        print('Tables loaded to database')
    else:
        df.to_csv('./parts.csv')
        df_holes.to_csv('./holes.csv')
        df_unreachable.to_csv('./unreachable_flags.csv')
        print('Tables saved locally as csv files')

def main(params):
    filename = params.filename
    write = params.write
    
    df = extract(filename)
    print("Extract phase finished")
    
    df_holes, df_unreachable = transform(df)
    print("Transform phase finished")
    
    print(df)
    print(df_holes)
    print(df_unreachable)
    
    load(df, df_holes, df_unreachable, write)
    print("Load phase finished")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hole data ETL')
    parser.add_argument('--filename', required=True, help='.parquet of part data')
    parser.add_argument('--write', action='store_true')

    args = parser.parse_args()

    main(args)