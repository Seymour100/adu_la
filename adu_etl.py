import numpy as np
import pandas as pd
from sodapy import Socrata
import censusdata
import geopandas as gpd
import requests
import re

class ETL:
    def __init__(self, la_city_database, la_city_usrn, la_city_psswrd, la_city_token, limit=8000):
        self.la_city_database = la_city_database,
        self.la_city_usrn = la_city_usrn,
        self.la_city_psswrd = la_city_psswrd,
        self.la_city_token = la_city_token
        with open(la_city_token, 'r') as f:
            api_token = f.readline().replace('\n','')
        with open(la_city_usrn, 'r') as f:
            usrn = f.readline().replace('\n','')
        with open(la_city_psswrd, 'r') as f:
            psswrd = f.readline().replace('\n','')
        self.client = Socrata('data.lacity.org', api_token, username=usrn, password=psswrd)

    def __repr__(self):
        return f'Query for database {self.la_city_database[0]}'

    # Create getters and setters for class attributes
    @property
    def la_city_database(self):
        return self._la_city_database

    @la_city_database.setter
    def la_city_database(self, la_city_database):
        if not isinstance(la_city_database[0], str):
            raise ValueError('"la_city_database" must be a string.')
        self._la_city_database = la_city_database[0]

    @property
    def la_city_usrn(self):
        return self._la_city_usrn

    @la_city_database.setter
    def la_city_usrn(self, la_city_usrn):
        if not isinstance(la_city_usrn[0], str):
            raise ValueError('"la_city_usrn" must be the path to your LA City Datahub username in str format.')
        self._la_city_usrn = la_city_usrn[0]

    @property
    def la_city_psswrd(self):
        return self._la_city_psswrd

    @la_city_database.setter
    def la_city_psswrd(self, la_city_psswrd):
        if not isinstance(la_city_psswrd[0], str):
            raise ValueError('"la_city_psswrd" must be the path to your LA City Datahub password in str format.')
        self._la_city_psswrd = la_city_psswrd[0]

    @property
    def la_city_token(self):
        return self._la_city_token

    @la_city_token.setter
    def la_city_token(self, la_city_token):
        if not isinstance(la_city_token[0], str):
            raise ValueError('"la_city_token" must be a the path to your LA City Datahub token in str format.')
        self._la_city_token = la_city_token[0]

    def get_records(self):
        self.record_count = self.client.get(self.la_city_database, select="COUNT(*)")
        return self.record_count

    def get_data(self):
        self.data_raw = self.client.get(self.la_city_database, limit=8000)
        self.data_df = pd.DataFrame(self.data_raw)
        return self.data_df

    def clean_data(self, data):
        data_pdf = data.copy()

        # Delete columns starting with ':@'
        bad_cols = [x for x in data_pdf.columns if re.search(':@', str(x))]
        for col in bad_cols:
            del data_pdf[col]

        data_pdf['census_tract'] = data_pdf['census_tract'].apply(lambda x: x.replace('.', ''))

        # Create one APN column
        data_pdf['APN'] = data_pdf['assessor_book']+'-'+data_pdf['assessor_page']+'-'+data_pdf['assessor_parcel']
        del data_pdf['assessor_book']
        del data_pdf['assessor_page']
        del data_pdf['assessor_parcel']

        # Extract latitude and longitude into separate columns
        data_pdf['latitude'] = data_pdf['location_1'].apply(lambda x: x['latitude'] if isinstance(x, dict) else None)
        data_pdf['latitude'] = data_pdf['latitude'].astype(float)
        data_pdf['longitude'] = data_pdf['location_1'].apply(lambda x: x['longitude'] if isinstance(x, dict) else None)
        data_pdf['longitude'] = data_pdf['longitude'].astype(float)
        del data_pdf['location_1']

        # Fix errors in census_tract
        data_pdf['census_tract'] = data_pdf['census_tract'].apply(lambda x: '137000' if x=='930401' else x)
        data_pdf['census_tract'] = data_pdf['census_tract'].apply(lambda x: '192410' if x=='192400' else x)
        data_pdf['census_tract'] = data_pdf['census_tract'].apply(lambda x: '219902' if x=='219900' else x)
        data_pdf['census_tract'] = data_pdf['census_tract'].apply(lambda x: '195903' if x=='195900' else x)
        data_pdf['census_tract'] = data_pdf['census_tract'].apply(lambda x: '269601' if x=='269600' else x)

        return data_pdf

    def median_income(self, data):
        data_pdf = data.copy()

        url='https://api.census.gov/data/2019/acs/acs5?get=B19013_001E&for=tract:*&in=state:06&in=county:037'
        median_income_raw = requests.get(url)
        median_income_df = pd.DataFrame(data=median_income_raw.json()[1:], columns=median_income_raw.json()[:1][0])
        median_income_df.rename(columns={'B19013_001E':'median_income','tract':'census_tract'}, inplace=True)
        del median_income_df['state']
        del median_income_df['county']
        data_pdf = pd.merge(data_pdf, median_income_df, on='census_tract', how='left')

        return data_pdf

    def parcels(self, data, parcels_loc):
        data_pdf = data.copy()

        parcels_df = pd.read_csv(parcels_loc)
        parcels_df['SQFTmainTot']=parcels_df['SQFTmain1'].fillna(0)+parcels_df['SQFTmain2'].fillna(0)+\
            parcels_df['SQFTmain3'].fillna(0)+parcels_df['SQFTmain4'].fillna(0)+parcels_df['SQFTmain5'].fillna(0)
        parcels_sub = parcels_df[['APN','SQFTmainTot', 'Shape.STArea()']]
        data_pdf = pd.merge(data_pdf, parcels_sub, on='APN', how='left')
        data_pdf.rename(columns={'SQFTmainTot':'building_size', 'Shape.STArea()':'lot_size'}, inplace=True)
        data_pdf['open_land']=data_pdf['lot_size']-data_pdf['building_size']

        return data_pdf

    def hillsides(self, data):
        data_pdf = data.copy()

        hills_gdf = gpd.read_file('https://opendata.arcgis.com/datasets/3ac07567df1c4f3b916ac258e426e3f5_6.geojson')
        data_gdf = gpd.GeoDataFrame(data_pdf, geometry=gpd.points_from_xy(data_pdf.longitude, data_pdf.latitude))
        data_gdf.crs = hills_gdf.crs
        del hills_gdf['OBJECTID']
        del hills_gdf['TOOLTIP']
        data_gdf = gpd.sjoin(data_gdf, hills_gdf, how='left', op="within")
        del data_gdf['index_right']
        del data_gdf['latitude']
        del data_gdf['longitude']
        data_gdf.rename(columns={'H_TYPE':'hillside'}, inplace=True)
        data_gdf['hillside']=data_gdf['hillside'].apply(lambda x: int(1) if isinstance(x,str) else np.nan)

        return data_gdf
