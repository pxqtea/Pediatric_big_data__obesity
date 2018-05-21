### test dates
#!/usr/bin/python
import csv
import pandas as pd
from configparser import ConfigParser
import zscore_overall_2 as zscoreconvert3
from sqlalchemy import create_engine
import sqlalchemy
import pandas.io.sql as pdsql 
import numpy as np

'''
Clean the Weight measurement: calculated the Z-score and modified z-score according to CDC(> age 2)/WHO (<=24 months) guidline

1. Fliter out measurements: 1. measurement data < Date of birth
2. Filter out negative values
3. Filter out duplicates 
4. Filter out data older than 20 years (ms.measurement_age_in_months <= 240, CDC does not have parameters for ages > 20 years)
4. Flag the implausible values according to the suggested cut-offs on modified z-score
5. Including IDs from the origianl measurement table: measurement_concept_id, measurement_date, measurement_datetime,  \
measurement_source_value, measurement_type_concept_id, operator_concept_id, unit_concept_id, unit_source_value, \
value_as_number, value_source_value, measurement_age_in_months, measurement_concept_name, unit_concept_name, \
site, measurement_id, site_id, person_id, visit_occurrence_id, provider_id
6. Weight measurement_concept_id = 3013762
7. Score the height table in the new schema
'''
def weight_cleaning(): 
    engine_local = create_engine('postgresql://uname:pwd***@reslnpbddb01.research.chop.edu:5432/pbd_cohort')

    ## sql query to select weight records: Fliter out measurements: 1. measurement data < Date of birth, negative values and duplicates 
    weight_sql = "WITH weight_cnt AS (\
                    SELECT ps.person_id, ps.birth_datetime, ps.gender_concept_id, ms.measurement_concept_id, ms.measurement_date, ms.measurement_datetime, \
                        ms.measurement_source_value, ms.measurement_type_concept_id, ms.operator_concept_id, ms.unit_concept_id, ms.unit_source_value, \
                        ms.value_as_number, ms.value_source_value, ms.measurement_age_in_months, ms.measurement_concept_name, ms.unit_concept_name, \
                        ms.site, ms.measurement_id, ms.site_id, ms.visit_occurrence_id, ms.provider_id, \
                        max(ms.measurement_datetime) over( PARTITION BY (ps.person_id, ms.measurement_date)) as maxtime  \
                    FROM chop_pbd_v27.measurement ms join chop_pbd_v27.person ps  \
                        on ps.person_id = ms.person_id AND ms.measurement_concept_id = 3013762 \
                        AND DATE (ms.measurement_date) > DATE (ps.birth_datetime) AND ms.value_as_number > 0 AND ms.measurement_age_in_months <=240 )\
                 SELECT ms.person_id, ms.gender_concept_id, ms.birth_datetime, ms.measurement_concept_id, ms.measurement_date, ms.measurement_datetime, \
                        ms.measurement_source_value, ms.measurement_type_concept_id, ms.operator_concept_id, ms.unit_concept_id, ms.unit_source_value, \
                        ms.value_as_number, ms.value_source_value, ms.measurement_age_in_months, ms.measurement_concept_name, ms.unit_concept_name, \
                        ms.site, ms.measurement_id, ms.site_id, ms.visit_occurrence_id, ms.provider_id \
                 FROM weight_cnt ms \
                 WHERE ms.measurement_datetime = ms.maxtime \
                ;"

    print("Start querying database... ")
    ## Query the database to find all weight records 
    weight_all = pd.read_sql_query(weight_sql, engine_local)
    #display(weight_all)
    
    ## Prepare data from Z-score calculation, separate male from female 
    outputs_df = pd.DataFrame()
    outputs_df = weight_all[['person_id', 'gender_concept_id', 'measurement_age_in_months','value_as_number' ]]
    outputs_df['agemos'] = weight_all['measurement_age_in_months']
    outputs_df = outputs_df.rename(index=str, columns={"measurement_age_in_months": "wagemos", "value_as_number": "weight"})
    #display(outputs_df)
    outputs_female = outputs_df.loc[outputs_df['gender_concept_id'] == 8532]
    outputs_male   = outputs_df.loc[outputs_df['gender_concept_id'] == 8507]
    
    outputs_female.loc[:, ('agemos', 'wagemos', 'weight')] = outputs_female.loc[:, ('agemos', 'wagemos', 'weight')].apply(pd.to_numeric)
    outputs_male.loc[:, ('agemos', 'wagemos', 'weight')] = outputs_male.loc[:, ('agemos', 'wagemos', 'weight')].apply(pd.to_numeric)

    ## Z-score calculation for female
    print("Start calculating Z-score for female... ")
    outputs_female_zscore = zscoreconvert3.zscore_cal(outputs_female, 'F', 'WT', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq' )
    outputs_female_zscore['person_id'] = outputs_female_zscore['person_id'].astype(int)
    #display(outputs_female_zscore)
    outputs_female_zscore = outputs_female_zscore[['person_id', 'agemos', 'weight', 'wz', 'wf', 'wbiv' ]]
    outputs_female_zscore = outputs_female_zscore.rename(index=str, columns={"agemos": "measurement_age_in_months", "weight": "value_as_number"})
    #display(outputs_female_zscore)
    
    outputs_female_zscore_all = pd.merge(weight_all, outputs_female_zscore, how='right', on=['person_id', 'measurement_age_in_months', 'value_as_number'])
    #display(outputs_female_zscore_all)
    
    ## Z-score calculation for male
    outputs_male_zscore = zscoreconvert3.zscore_cal(outputs_male, 'M', 'WT', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq' )
    outputs_male_zscore['person_id'] = outputs_male_zscore['person_id'].astype(int)
    #display(outputs_male_zscore)
    outputs_male_zscore = outputs_male_zscore[['person_id', 'agemos', 'weight', 'wz', 'wf', 'wbiv' ]]
    outputs_male_zscore = outputs_male_zscore.rename(index=str, columns={"agemos": "measurement_age_in_months", "weight": "value_as_number"})
    #display(outputs_male_zscore)
    
    outputs_male_zscore_all = pd.merge(weight_all, outputs_male_zscore, how='right', on=['person_id', 'measurement_age_in_months', 'value_as_number'])
    #display(outputs_male_zscore_all)
    
    ## Combine male/female into one 
    weight_zscore_all = outputs_female_zscore_all.append(outputs_male_zscore_all)
    weight_zscore_all = weight_zscore_all.rename(index=str, columns = {'wz': 'zscore', 'wf': 'modified_zscore', 'wbiv': 'biv'})
    
    #display(weight_zscore_all)
    print(len(weight_zscore_all))
    #weight_female_df = weight_all_df[]
    
    ## write results into the database 
    #engine_local.execute('DROP TABLE IF EXISTS obesity_pangx_v27.weight_zscore_flag')
    weight_zscore_all.to_sql('weight_zscore_flag', engine_local, index = False, schema = 'obesity_pangx_v27')

### Creat new schema for the new weight table
weight_cleaning()

#conn.show_data(weight_sql, conn_local)                 




