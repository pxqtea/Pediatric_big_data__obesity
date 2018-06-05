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
Clean the Height measurement: calculated the Z-score and modified z-score according to CDC(> age 2)/WHO (<=24 months) guidline

1. Fliter out measurements: 1. measurement data < Date of birth
2. Filter out negative values
3. Filter out duplicates 
4. Filter out data older than 20 years (ms.measurement_age_in_months <= 240, CDC does not have parameters for ages > 20 years)
4. Flag the implausible values according to the suggested cut-offs on modified z-score
5. Including IDs from the origianl measurement table: measurement_concept_id, measurement_date, measurement_datetime,  \
measurement_source_value, measurement_type_concept_id, operator_concept_id, unit_concept_id, unit_source_value, \
value_as_number, value_source_value, measurement_age_in_months, measurement_concept_name, unit_concept_name, \
site, measurement_id, site_id, person_id, visit_occurrence_id, provider_id
6. Height measurement_concept_id = 3023540
7. Score the height table in the new schema
'''
def height_cleaning(): 
    engine_local = create_engine('postgresql://pangx:pxq@130986pxq@reslnpbddb01.research.chop.edu:5432/pbd_cohort')

    ## sql query to select height records: Fliter out measurements: 1. measurement data < Date of birth, negative values and duplicates 
    height_sql = "WITH height_cnt AS (\
                    SELECT distinct(ps.person_id, ms.measurement_date, ms.measurement_datetime, ms.value_source_value ) as dist_record, \
                        ps.person_id, ps.birth_datetime, ps.gender_concept_id, ms.measurement_concept_id, ms.measurement_date, ms.measurement_datetime, \
                        ms.measurement_source_value, ms.measurement_type_concept_id, ms.operator_concept_id, ms.unit_concept_id, ms.unit_source_value, \
                        ms.value_as_number, ms.value_source_value, ms.measurement_age_in_months, ms.measurement_concept_name, ms.unit_concept_name, \
                        ms.site, ms.measurement_id, ms.site_id, ms.visit_occurrence_id, \
                        max(ms.measurement_datetime) over( PARTITION BY (ps.person_id, ms.measurement_date)) as maxtime,  \
                        count(*) OVER(PARTITION BY (ms.person_id, ms.measurement_date, ms.measurement_datetime)) as cnt, \
                        count(*) OVER(PARTITION BY (ms.person_id, ms.measurement_date, ms.measurement_datetime, ms.value_source_value  )) as cnt2 \
                    FROM chop_pbd_v27.measurement ms join chop_pbd_v27.person ps  \
                        on ps.person_id = ms.person_id AND ms.measurement_concept_id = 3023540 \
                        AND DATE (ms.measurement_date) > DATE (ps.birth_datetime) AND ms.value_as_number > 0 AND ms.measurement_age_in_months <=240 \
                        limit 5000 )\
                 SELECT distinct(ms.person_id, ms.measurement_date, ms.measurement_datetime, ms.value_source_value ) as dist_record, \
                        ms.person_id, ms.gender_concept_id, ms.birth_datetime, ms.measurement_concept_id, ms.measurement_date, ms.measurement_datetime, \
                        ms.value_source_value::decimal * 2.54 as height_cm, ( DATE (ms.measurement_datetime) - DATE (ms.birth_datetime) ) as measurement_age_in_days,  \
                        ms.measurement_source_value, ms.measurement_type_concept_id, ms.operator_concept_id, ms.unit_concept_id, ms.unit_source_value, \
                        ms.value_as_number, ms.value_source_value, ms.measurement_age_in_months, ms.measurement_concept_name, ms.unit_concept_name, \
                        ms.site, ms.measurement_id, ms.site_id, ms.visit_occurrence_id \
                 FROM height_cnt ms \
                 WHERE ms.measurement_datetime = ms.maxtime AND ms.cnt = ms.cnt2 \
                ;"

    print("Start querying database... ")
    ## Query the database to find all height records 
    height_all = pd.read_sql_query(height_sql, engine_local)
    height_all.drop('dist_record', axis=1, inplace=True)
    print(len(height_all))
    
    ## Prepare data from Z-score calculation, separate male from female 
    outputs_df = pd.DataFrame()
    outputs_df = height_all[['person_id', 'gender_concept_id', 'measurement_age_in_months', 'measurement_age_in_days', 'height_cm' ]]
    outputs_df['agemos'] = height_all['measurement_age_in_months']
    outputs_df = outputs_df.rename(index=str, columns={"measurement_age_in_months": "hagemos", "height_cm": "height", "measurement_age_in_days":"hagedays"})
    #display(outputs_df)
    
    outputs_female = outputs_df.loc[outputs_df['gender_concept_id'] == 8532]
    outputs_male   = outputs_df.loc[outputs_df['gender_concept_id'] == 8507]
    print(len(outputs_female))
    print(len(outputs_male))
    outputs_female.loc[:, ('agemos', 'hagemos', 'height', 'hagedays')] = outputs_female.loc[:, ('agemos', 'hagemos', 'height', 'hagedays')].apply(pd.to_numeric)
    outputs_male.loc[:, ('agemos', 'hagemos', 'height', 'hagedays')] = outputs_male.loc[:, ('agemos', 'hagemos', 'height', 'hagedays')].apply(pd.to_numeric)

    ## Z-score calculation for female
    print("Start calculating Z-score for female... ")
    outputs_female_zscore = zscoreconvert3.zscore_cal(outputs_female, 'F', 'HT', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq' )
    outputs_female_zscore['person_id'] = outputs_female_zscore['person_id'].astype(int)
    #display(outputs_female_zscore)
    outputs_female_zscore = outputs_female_zscore[['person_id', 'agemos', 'height', 'hz', 'hf', 'hbiv' ]]
    outputs_female_zscore = outputs_female_zscore.rename(index=str, columns={"agemos": "measurement_age_in_months", "height": "height_cm"})
    print(len(outputs_female_zscore))
    
    outputs_female_zscore_all = pd.merge(height_all, outputs_female_zscore, how='right', on=['person_id', 'measurement_age_in_months', 'height_cm'])
    #display(outputs_female_zscore_all)
    
    ## Z-score calculation for male
    outputs_male_zscore = zscoreconvert3.zscore_cal(outputs_male, 'M', 'HT', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq' )
    outputs_male_zscore['person_id'] = outputs_male_zscore['person_id'].astype(int)
    #display(outputs_male_zscore)
    outputs_male_zscore = outputs_male_zscore[['person_id', 'agemos', 'height', 'hz', 'hf', 'hbiv' ]]
    outputs_male_zscore = outputs_male_zscore.rename(index=str, columns={"agemos": "measurement_age_in_months", "height": "height_cm", })
    print(len(outputs_male_zscore))
    
    outputs_male_zscore_all = pd.merge(height_all, outputs_male_zscore, how='right', on=['person_id', 'measurement_age_in_months', 'height_cm'])
    #display(outputs_male_zscore_all)
    
    ## Combine male/female into one 
    height_zscore_all = outputs_female_zscore_all.append(outputs_male_zscore_all)
    height_zscore_all = height_zscore_all.rename(index=str, columns = {'hz': 'zscore', 'hf': 'modified_zscore', 'hbiv': 'biv'})
    
    #display(height_zscore_all)
    print(len(height_zscore_all))
    #height_female_df = height_all_df[]
    
    ## write results into the database 
    engine_local.execute('DROP TABLE IF EXISTS obesity_pangx_v27.heightconvert_zscore_flag_t')
    height_zscore_all.to_sql('heightconvert_zscore_flag_t', engine_local, index = True, schema = 'obesity_pangx_v27')
    #height_zscore_all.to_sql('height_zscore_flag', engine_local, schema = 'v24_model1')

### Creat new schema for the new height table
height_cleaning()

#conn.show_data(height_sql, conn_local)                 

