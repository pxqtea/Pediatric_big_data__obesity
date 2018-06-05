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
from IPython.display import display

'''
Creat and clean the BMI measurement: calculated the Z-score and modified z-score according to CDC(> age 2)/WHO (<=24 months) guidline
(use same day height/weight measurement)
CHECKED: The same day height/weight measurements all have the same visit_occurrence_id and site_id

1. Fliter out measurements: 1. measurement data < Date of birth
2. Filter out negative values
3. Filter out duplicates 
4. Filter out data older than 20 years (ms.measurement_age_in_months <= 240, CDC does not have parameters for ages > 20 years)
4. Flag the implausible values according to the suggested cut-offs on modified z-score
5. Including IDs from the origianl measurement table: measurement_concept_id, measurement_date, measurement_datetime,  \
measurement_source_value, measurement_type_concept_id, operator_concept_id, unit_concept_id, unit_source_value, \
value_as_number, value_source_value, measurement_age_in_months, measurement_concept_name, unit_concept_name, \
site, measurement_id, site_id, person_id, visit_occurrence_id, provider_id
6. Weight measurement_concept_id = 3023540
7. Score the bmi table in the new schema
'''
def bmi_cleaning(): 
    engine_local = create_engine('postgresql://pangx:pxq@130986pxq@reslnpbddb01.research.chop.edu:5432/pbd_cohort')

    ## sql query to select height records: Fliter out measurements: 1. measurement data < Date of birth, negative values and duplicates 
    bmi_sql = "SELECT ms.person_id, ms.gender_concept_id, ms.birth_datetime, ms.weight_kg weight, h.height_cm height, \
                        2000000043 measurement_concept_id, ms.measurement_date, ms.measurement_datetime, \
                        ms.measurement_type_concept_id, ms.operator_concept_id, ms.unit_concept_id,  \
                        ms.measurement_age_in_months, ms.measurement_concept_name, ms.unit_concept_name, \
                        ms.site, ms.measurement_id, ms.site_id, ms.visit_occurrence_id, ms.provider_id, \
                        ( DATE (ms.measurement_datetime) - DATE (ms.birth_datetime) ) as measurement_age_in_days  \
                  FROM obesity_pangx_v27.heightconvert_zscore_flag h inner join obesity_pangx_v27.weightconvert_zscore_flag ms \
                  ON ms.person_id = h.person_id AND ms.measurement_date = h.measurement_date \
                  ;"

    print("Start querying database... ")
    ## Query the database to find all height records 
    bmi_all = pd.read_sql_query(bmi_sql, engine_local)
    #display(bmi_all)
    
    ## Prepare data from Z-score calculation, separate male from female 
    outputs_df = pd.DataFrame()
    outputs_df = bmi_all[['person_id', 'gender_concept_id', 'measurement_age_in_months', 'weight', 'height' ]]
    outputs_df['wagemos'] = bmi_all['measurement_age_in_months']
    outputs_df['hagemos'] = bmi_all['measurement_age_in_months']
    outputs_df['wagedays'] = bmi_all['measurement_age_in_days']
    outputs_df['hagedays'] = bmi_all['measurement_age_in_days']
    outputs_df = outputs_df.rename(index=str, columns={"measurement_age_in_months": "agemos"})
    #display(outputs_df)
    outputs_female = outputs_df.loc[outputs_df['gender_concept_id'] == 8532]
    outputs_male   = outputs_df.loc[outputs_df['gender_concept_id'] == 8507]
    
    outputs_female.loc[:, ('agemos', 'hagemos', 'hagedays', 'height', 'wagemos','wagedays','weight')] = outputs_female.loc[:, ('agemos', 'hagemos','hagedays', 'height', 'wagemos','wagedays', 'weight')].apply(pd.to_numeric)
    outputs_male.loc[:, ('agemos', 'hagemos', 'hagedays','height', 'wagemos', 'wagedays', 'weight')] = outputs_male.loc[:, ('agemos', 'hagemos','hagedays', 'height', 'wagemos','wagedays', 'weight')].apply(pd.to_numeric)

    ## Z-score calculation for female
    print("Start calculating Z-score for female... ")
    outputs_female_zscore = zscoreconvert3.zscore_cal(outputs_female, 'F', 'HW1', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq' )
    outputs_female_zscore['person_id'] = outputs_female_zscore['person_id'].astype(int)
    #display(outputs_female_zscore)
    outputs_female_zscore = outputs_female_zscore[['person_id', 'agemos', 'height', 'hz', 'hf', 'hbiv', 'weight', 'wz', 'wf', 'wbiv','bmi', 'bz', 'bf', 'bbiv' ]]
    outputs_female_zscore = outputs_female_zscore.rename(index=str, columns={"agemos": "measurement_age_in_months"})
    display(outputs_female_zscore)
    
    outputs_female_zscore_all = pd.merge(bmi_all, outputs_female_zscore, how='right', on=['person_id', 'measurement_age_in_months', 'weight', 'height'])
    display(outputs_female_zscore_all)
    
    ## Z-score calculation for male
    outputs_male_zscore = zscoreconvert3.zscore_cal(outputs_male, 'M', 'HW1', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq' )
    outputs_male_zscore['person_id'] = outputs_male_zscore['person_id'].astype(int)
    #display(outputs_male_zscore)
    outputs_male_zscore = outputs_male_zscore[['person_id', 'agemos', 'height', 'hz', 'hf', 'hbiv', 'weight', 'wz', 'wf', 'wbiv','bmi', 'bz', 'bf', 'bbiv']]
    outputs_male_zscore = outputs_male_zscore.rename(index=str, columns={"agemos": "measurement_age_in_months"})
    display(outputs_male_zscore)
    
    outputs_male_zscore_all = pd.merge(bmi_all, outputs_male_zscore, how='right', on=['person_id', 'measurement_age_in_months', 'weight', 'height'])
    display(outputs_male_zscore_all)
    
    ## Combine male/female into one 
    bmi_zscore_all = outputs_female_zscore_all.append(outputs_male_zscore_all)
    bmi_zscore_all = bmi_zscore_all.rename(index=str, columns = {'hz': 'height_zscore', 'hf': 'height_modified_zscore', 'hbiv': 'height_biv', \
                        'wz': 'weight_zscore', 'wf': 'weight_modified_zscore', 'wbiv': 'weight_biv', 'bz': 'bmi_zscore', 'bf': 'bmi_modified_zscore', 'bbiv': 'bmi_biv'})
    
    display(bmi_zscore_all)
    print(len(bmi_zscore_all))
    #bmi_female_df = bmi_all_df[]
    
    ## write results into the database 
    engine_local.execute('DROP TABLE IF EXISTS obesity_pangx_v27.heightconvert_weightconvert_bmi_zscore_flag')
    bmi_zscore_all.to_sql('heightconvert_weightconvert_bmi_zscore_flag', engine_local,index = False, schema = 'obesity_pangx_v27')

### Creat new schema for the new bmi table
bmi_cleaning()

#conn.show_data(bmi_sql, conn_local)                 


