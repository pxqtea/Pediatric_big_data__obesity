'''
Created on Sep 18, 2017

@author: pangx
'''
import pandas as pd
import math
import scipy.stats as st
import numpy as np
import pandas as pd
from CDC_Calculator_overall import CDCCalculator 
from WHO_Calculator_overall import WHOCalculator
from bokeh.layouts import column
from datetime import datetime
from numba.types import none
import math
import csv
from operator import itemgetter
import operator
from blaze import inf





 ## The input file with columns (agemons, agedays, height, weight, dob, measurement date, etc... )
global INPUT, CDCPARAM, WHOPARAM, OUTOUT, WWAL, WWAH, WHAL, WHAH, WBMIL, WBMIH, CWAL, CWAH, CHAL, CHAH, CBMIL, CBMIH, params, outs, variablist 
WWAL = -6; WWAH = 5; WHAL = -6; WHAH = 6; WBMIL = -5; WBMIH = 5  ### WHO Z_value cutoffs
CWAL = -5; CWAH = 8; CHAL = -5; CHAH = 4; CBMIL = -4; CBMIH = 8  ### CDC Z_value cutoffs
outs, cdcouts, whoouts = [], [], []
global intitledic, tmpouts
tmpouts = []


params = ['L', 'M', 'S']
variablist = ['HT', 'WT', 'BMI']
 
titleline = ['id', 'agemos', 'height',  'date','hz', 'hp', 'hf', \
             'hq95', 'hpct95', 'hq50', 'hbiv', 'hmdf', 'hmdf3', 'hmdf4']
  
cdctitle =  ['id', 'agemos', 'weight', 'hagemos',  'height', 'obmiz', 'sex']
whotitle =  ['id', 'agedays', 'weight', 'hagemos',  'height', 'obmiz', 'sex']

## input data structure: 
# person_id, measurement_id(wt/ht), dob, agemons, agedays, height, weight,  
##output data structure:
#person_id, measurement_id, dob, agemons, agedays, height, weight, bmi, height_z, weight_z, bmi_z, bivh, bivw, bivbmi, bmi50, bmi95
##cdc data structure:
##format of the CDC file (age from 23.5 to 239.5 month ) and weight_for_height with height in (45, 121cm)
#sex, agemos1/2, _L/M/S(HT/WT/BMI/HC)1/2

##WHO data structure:
##format of the WHO file (included age upto 1856, but will be used on kids younger than 2 only, and weight_for_height with height in (45, 110cm)): 
#sex, agedays, _bmi/height/weight/headc/_l/m/s (also have subscapular/triceps skinfold thickness and arm circumference measurments)



def cdc_run(cdc_input, gender, cdcfile, mode):
    outs = []
    #global tmpouts
    tmpout = []

    ##cdc_input as a list sorted on wagemos
    print("run CDC calculator for age > 24 months for ", len(cdc_input), "measurements")
    cdcparams = pd.read_csv(cdcfile + '.csv') 
    cdcparams = cdcparams.loc[(cdcparams['SEX'] == gender)]
    cdcsize = len(cdcparams)
    if(cdcsize == 0): print("cdc parameter file is empty"); exit(-1)
    cdcparams.reset_index(drop=True, inplace=True)
    cdcparams = cdcparams.values.tolist()
    
    '''
    ### for WHO calculations only
    if mode.find('2') != -1:
        cdcparamsHW = pd.read_csv(cdcfile + '_wh.csv')
        cdcparamsHW = cdcparamsHW.loc[(cdcparamsHW['SEX'] == gender)]
        cdcsizeHW = len(cdcparamsHW)
        if(cdcsizeHW == 0): print("Height_weight parameter file is empty"); exit(-1)
        cdcparamsHW.reset_index(drop=True, inplace=True)
        cdcparamsHW = cdcparamsHW.values.tolist()
    '''
    cdc = CDCCalculator()
    
    if mode.find('H') != -1 and mode.find('W') != -1 and mode.find('C') != -1 and mode.find('2') != -1:
        ### for the full mode calculations
    
        wparam, hparam, bparam, hcparam, hwparam = [], [], [], [], []
        hptr = 0
        hwptr = 0
            
        for row in cdc_input:
            out = []
            hage = row[intitledic['hagemos']]; wage = row[intitledic['wagemos']]; hcage = row[intitledic['hcagemos']]
            hptr =  math.ceil(hage - 0.5) #int(int(hage + 1))/2 + 1
            #print(hptr)
            wptr =  math.ceil(wage - 0.5)
            hcptr =  math.ceil(hcage - 0.5)
            #hwptr = math.ceil(row[intitledic['height']] - 45.5)+ 1 
            
                     
            if (row[intitledic['height']] <121.5  and row[intitledic['height']] >= 45 ):
                hwptr = math.ceil(row[intitledic['height']] - 45.5)+ 1 
                if (hwptr < cdcsize): 
                    hwparam.append( cdcparams[hwptr][36:39] + cdcparams[hwptr][40:43] + [cdcparams[hwptr][35]] + [row[intitledic['height']]] + [row[intitledic['weight']]])
                else: print("Weight_height parameter unavailable"); exit(-1)
            else: hwparam.append([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, row[intitledic['height']], row[intitledic['weight']]] )
            
            if (hptr == cdcsize) or (wptr == cdcsize) or (hcptr == cdcsize) or (hwptr == cdcsize) : print("age out of CDC_ref range"); exit(-1)
            weight = row[intitledic['weight']]
            #height = row[4]
            height = row[intitledic['height']]  #cdc.height_stat(row[4])
            if (height <= 0 or weight <=0 ):
                print(row); exit(-1)
            bmi = cdc.bmi_cal(height, weight, wage)         
            #woverh = weight / height  
                     
            wparam.append( cdcparams[wptr][17:23] + [cdcparams[wptr][2]] + [wage] + [row[intitledic['weight']]])
            hparam.append( cdcparams[hptr][11:17] + [cdcparams[hptr][2]] + [hage] + [row[intitledic['height']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() 
            bparam.append( cdcparams[wptr][23:29] + [cdcparams[wptr][2]] + [wage] + [bmi])
            hcparam.append( cdcparams[hcptr][29:35] + [cdcparams[hcptr][2]] + [hcage] + [row[intitledic['headc']]])
            
            #hwparam.append( cdcparamsHW[hwptr][36:39] + cdcparamsHW[hwptr][40:43] + [cdcparamsHW[hwptr][35]] + [wage] + row[intitledic['weight']])
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)
            
            # out += row[i for (i, x) in intitledic]

        htmp = cdc.cal_zscore(hparam, 'HT' )
        wtmp = cdc.cal_zscore(wparam, 'WT')
        btmp = cdc.cal_zscore(bparam, 'BMI')
        hctmp = cdc.cal_zscore(hcparam, 'HC')
        hwtmp = cdc.cal_zscore(hwparam, 'HW')
        
        for items in zip(outs, htmp, wtmp, btmp, hctmp, hwtmp):
            tmpout = []
            #print(items)
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)
        
        
    elif mode.find('H') != -1 and mode.find('W') != -1 and mode.find('C') != -1:
        ### for H/W/BMI/headC calculations no height_weight calculation
        for row in cdc_input:            
            out = []
            hage = row[intitledic['hagemos']]; wage = row[intitledic['wagemos']]; hcage = row[intitledic['hcagemos']]
            hptr =  math.ceil(hage - 0.5) #int(int(hage + 1))/2 + 1
            wptr =  math.ceil(wage - 0.5)
            hcptr =  math.ceil(hcage - 0.5)            

            if (hptr == cdcsize) or (wptr == cdcsize) or (hcptr == cdcsize) : print("age out of CDC_ref range"); exit(-1)
            weight = row[intitledic['weight']]
            #height = row[4]
            height = row[intitledic ['height']]  #cdc.height_stat(row[4])
            if (height <= 0 or weight <=0 ):
                print(row); exit(-1)
            bmi = cdc.bmi_cal(height, weight, wage)            
            wparam.append( cdcparams[wptr][17:23] + [cdcparams[wptr][2]] + [wage] + [row[intitledic['weight']]])
            hparam.append( cdcparams[hptr][11:17] + [cdcparams[hptr][2]] + [hage] + [row[intitledic['height']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() 
            bparam.append( cdcparams[wptr][23:29] + [cdcparams[wptr][2]] + [wage] + [bmi])
            hcparam.append( cdcparams[hcptr][29:35] + [cdcparams[hcptr][2]] + [hcage] + [row[intitledic['headc']]])
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)
            
        htmp = cdc.cal_zscore(hparam, 'HT' )
        wtmp = cdc.cal_zscore(wparam, 'WT')
        btmp = cdc.cal_zscore(bparam, 'BMI')
        hctmp = cdc.cal_zscore(hcparam, 'HC')
        
        for items in zip(outs, htmp, wtmp, btmp, hctmp):
            tmpout = []
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)

        
    elif mode.find('H') != -1 and mode.find('W') != -1 and mode.find('2') != -1:
        ### for H/W/BMI/height_weight calculations
        wparam, hparam, bparam, hwparam = [], [], [], []
        for row in cdc_input:
            out = []
            hage = row[intitledic['hagemos']]; wage = row[intitledic['wagemos']]
            hptr =  math.ceil(hage - 0.5)  #int(int(hage + 1))/2 + 1
            wptr =  math.ceil(wage - 0.5) 
            #hwptr = math.ceil(row[intitledic['height']] - 45.5)+ 1 
            
            if (row[intitledic['height']] <121.5  and row[intitledic['height']] >= 45 ):
                hwptr = math.ceil(row[intitledic['height']] - 45.5)+ 1 
                if (hwptr < cdcsize): 
                    hwparam.append( cdcparams[hwptr][36:39] + cdcparams[hwptr][40:43] + [cdcparams[hwptr][35]] + [row[intitledic['height']]] + [row[intitledic['weight']]])
                else: print("Weight_height parameter unavailable"); exit(-1)
            else: hwparam.append([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, row[intitledic['height']], row[intitledic['weight']]] )
            
            #if (hptr == cdcsize) or (wptr == cdcsize) or hwptr == len(cdcparamsHW) : print("age out of CDC_ref range"); exit(-1)
            if (hptr == cdcsize) or (wptr == cdcsize) or (hwptr == cdcsize) : print("age out of CDC_ref range"); exit(-1)
            weight = row[intitledic['weight']]
            #height = row[4]
            height = row[intitledic['height']]  #cdc.height_stat(row[4])
            if (height <= 0 or weight <=0 ):
                print(row); exit(-1)
            bmi = cdc.bmi_cal(height, weight, wage)    
            #woverh = weight / height        
            wparam.append( cdcparams[wptr][17:23] + [cdcparams[wptr][2]] + [wage] + [row[intitledic['weight']]])
            hparam.append( cdcparams[hptr][11:17] + [cdcparams[hptr][2]] + [hage] + [row[intitledic['height']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() 
            bparam.append( cdcparams[wptr][23:29] + [cdcparams[wptr][2]] + [wage] + [bmi])
            #hwparam += ( cdcparamsHW[hwptr][36:39] + cdcparamsHW[hwptr][40:43] + [cdcparamsHW[hwptr][35]] + [wage] + [row[intitledic['weight']]])
            #hwparam.append( cdcparams[hwptr][36:39] + cdcparams[hwptr][40:43] + [cdcparams[hwptr][35]] + [row[intitledic['height']]] + [row[intitledic['weight']]])
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)
        
        #print(hparam)    
        htmp = cdc.cal_zscore(hparam, 'HT' )
        wtmp = cdc.cal_zscore(wparam, 'WT')
        btmp = cdc.cal_zscore(bparam, 'BMI')
        hwtmp = cdc.cal_zscore(hwparam, 'HW')
        #print(len(outs), len(htmp), len(wtmp), len(btmp), len(hwtmp))
        #outs = list(zip(outs, htmp, wtmp, btmp, hwtmp))
        #outs = [item for items in zip(outs, htmp, wtmp, btmp, hwtmp) for item in items]
        #outs = np.hstack((outs, htmp, wtmp, btmp, hwtmp))
        #outs = outs + htmp + wtmp + btmp + hwtmp
        #outs = outs.flate()
        for items in zip(outs, htmp, wtmp, btmp, hwtmp):
            tmpout = []
            #print(items)
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)
        
    elif mode.find('H') != -1 and mode.find('W') != -1 and mode.find('1') != -1:
        wparam, hparam, bparam = [], [], []
        for row in cdc_input:
            out = []
            hage = row[intitledic['hagemos']]; wage = row[intitledic['wagemos']]
            hptr =  math.ceil(hage - 0.5) #int(int(hage + 1))/2 + 1
            wptr =  math.ceil(wage - 0.5)            

            if (hptr == cdcsize) or (wptr == cdcsize) : print("age out of CDC_ref range"); exit(-1)
            weight = row[intitledic['weight']]
            #height = row[4]
            height = row[intitledic['height']]  #cdc.height_stat(row[4])
            if (height <= 0 or weight <=0 ):
                print(row); exit(-1)
            bmi = cdc.bmi_cal(height, weight, wage)
            wparam.append( cdcparams[wptr][17:23] + [cdcparams[wptr][2]] + [wage] + [row[intitledic['weight']]])
            hparam.append( cdcparams[hptr][11:17] + [cdcparams[hptr][2]] + [hage] + [row[intitledic['height']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() 
            bparam.append( cdcparams[wptr][23:29] + [cdcparams[wptr][2]] + [wage] + [bmi])
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)
            
        htmp = cdc.cal_zscore(hparam, 'HT' )
        wtmp = cdc.cal_zscore(wparam, 'WT')
        btmp = cdc.cal_zscore(bparam, 'BMI')
        for items in zip(outs, htmp, wtmp, btmp):
            tmpout = []
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)

    elif mode.find('W') != -1:
        wparam = []
        for row in cdc_input:
            out = []
            wage = row[intitledic['wagemos']]
            wptr =  math.ceil(wage - 0.5)            

            if (wptr == cdcsize): print("age out of CDC_ref range"); exit(-1)
            weight = row[intitledic['weight']]
            #height = row[4]
            if ( weight <=0 ):
                print(row); exit(-1)
            wparam.append( cdcparams[wptr][17:23] + [cdcparams[wptr][2]] + [wage] + [row[intitledic['weight']]])
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)
            
        wtmp = cdc.cal_zscore(wparam, 'WT')
        for items in zip(outs, wtmp):
            tmpout = []
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)
        
    elif mode.find('H') != -1:
        hparam = []
        for row in cdc_input:
            out = []
            hage = row[intitledic['hagemos']]
            hptr =  math.ceil(hage - 0.5) #int(int(hage + 1))/2 + 1
            
            if (hptr == cdcsize): print("age out of CDC_ref range"); exit(-1)
            height = row[intitledic['height']]  #cdc.height_stat(row[4])
            if (height <= 0 ):
                print(row); exit(-1)
            hparam.append( cdcparams[hptr][11:17] + [cdcparams[hptr][2]] + [hage] + [row[intitledic['height']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() 
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)
            
        htmp = cdc.cal_zscore(hparam, 'HT' )
        for items in zip(outs, htmp):
            tmpout = []
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)

    elif mode.find('C') != -1:
        hcparam = []
        for row in cdc_input:
            out = []
            hcage = row[intitledic['hcagemos']]
            hcptr =  math.ceil(hcage - 0.5) #int(int(hage + 1))/2 + 1

            if (hcptr == cdcsize): print("age out of CDC_ref range"); exit(-1)
            headc = row[intitledic['headc']]  #cdc.height_stat(row[4])
            #if (headc <= 0 ):
            #    print(row); exit(-1)
            hcparam.append( cdcparams[hcptr][29:35] + [cdcparams[hcptr][2]] + [hcage] + [row[intitledic['headc']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() 
            out = [row[intitledic['person_id']]] + [row[intitledic['agemos']]]
            outs.append(out)

        hctmp = cdc.cal_zscore(hcparam, 'HC' )
        for items in zip(outs, hctmp):
            tmpout = []
            for item in items:
                tmpout += item
            tmpouts.append(tmpout)


        ### for height calculations only 

        #outs.append([row[intitledic['person_id']]] + [row[intitledic['agemos']]] + htmp + wtmp + btmp + hwtmp + hctmp)    ## the new bmi is also calculated with CDC method                

def who_run(who_input, gender, whofile, whofile_hw, mode):
    
    print("run WHO calculator for age <= 24 months for ", len(who_input), "measurements" )

    whoparams = pd.read_csv(whofile + '.csv') 
    whoparams = whoparams.loc[(whoparams['sex'] == gender)]
    whoparams.reset_index(drop=True, inplace=True)
    whosize = len(whoparams)
    if whosize == 0:
        print("parameter file is empty"); exit(-1)
    whoparams = whoparams.values.tolist()
    
    if mode.find('2') != -1:
        whoparamsHW = pd.read_csv(whofile_hw + '.csv')
        whoparamsHW = whoparamsHW.loc[(whoparamsHW['sex'] == gender)]
        whoparamsHW.reset_index(drop=True, inplace=True)
        whosizeHW = len(whoparamsHW)
        if whosizeHW  == 0: print("Weight_height parameter file is empty"); exit(-1)
        whoparamsHW = whoparamsHW.values.tolist()

    global tmpouts
    out = [];     outs = []
    #global tmpouts
    tmpout = []

    who = WHOCalculator()
    wparam, hparam, bparam, hcparam, hwparam = [], [], [], [], []
    wtmp, htmp, btmp, hctmp, hwtmp = [], [], [], [], []
    wptr = 0; hptr = 0; bptr = 0; hcptr = 0; hwptr = 0
        
    
    for row in who_input: 
        out = []
        if mode.find('H') != -1: 
            # for height calculation
            #print("Height calculation")
            hage = round(row[intitledic['hagemos']] * 30.4)
            if (hage < whosize):
                hparam.append(whoparams[hage][20:23] + [hage] + [row[intitledic['height']]]) # + cdcparams.loc[hptr:hptr, 1:1].values.tolist() =
            elif(hage == whosize): print("age out of WHO_ref range"); exit(-1)    
            '''            if mode.find('W') != -1: 
                print("BMI calculations")
                wage = round(row[1] * 30.4)
                #print(row)
                if ( wage < whosize):
                    #print(wage, whoparams[wage])
                    wparam.append(whoparams[wage][17:20] + [whoparams[wage][1]])
                    bparam = whoparams[wage][2:5] + [whoparams[wage][1]]
                     #+ cdcparams.loc[wptr:wptr, 1:1].values.tolist()             
                elif(wage == whosize): print("age out of WHO_ref range"); exit(-1)
            '''   
        if mode.find('W') != -1:
            #print("Weight calculation")
            wage = round( row[intitledic['wagemos']] * 30.4)
            #print(row)
            if ( wage < whosize):
                #print(wage, whoparams[wage])
                wparam.append(whoparams[wage][17:20] +  [wage] + [row[intitledic['weight']]])
                 #+ cdcparams.loc[wptr:wptr, 1:1].values.tolist()             
            elif(wage == whosize): print("age out of WHO_ref range"); exit(-1)
            # for weight calculation
        if mode.find('2') != -1: 
            height = row[intitledic['height']]
            if height >= 45 and height <= 110 : 
                hwptr = int((height - 45 ) * 100)
                if hwptr < whosizeHW:
                    hwparam.append(whoparamsHW[hwptr][2:5] +  [height] + [row[intitledic['weight']]])
                else: print("Weight_height parameter unavailable"); exit(-1)
            else: hwparam.append([np.nan, np.nan, np.nan, height, row[intitledic['weight']] ])
        if mode.find('C') != -1: 
            hcage = round(row[intitledic['hcagemos']] * 30.4)
            if (hcage < whosize):
                hcparam.append(whoparams[hcage][14:17] +  [hcage] + [row[intitledic['headc']]])
            else: print("age out of WHO_ref range"); exit(-1)
        if mode.find('H') != -1 and mode.find('W') != -1 :
            wage = round(row[intitledic['wagemos']] * 30.4)
            height = row[intitledic['height']]; weight = row[intitledic['weight']]; 
            bmi = who.bmi_cal(height, weight); 
            bparam.append(whoparams[wage][2:5] +  [wage] +  [bmi] )
            
        out = [row[intitledic['person_id']],  row[intitledic['agemos']]]
        outs.append(out)
      
    outlist = []  
    
    if len(hparam) != 0:
        htmp = who.cal_zscore(hparam, 'HT' )
        if (len(outs) != 0): 
            outlist = []
            for items in zip(outs, htmp):
                tmpout = []
                for item in items:
                    tmpout += item
                outlist.append(tmpout)
            outs = outlist
        else: outs = htmp 
        
    if len(wparam) != 0:
        wtmp = who.cal_zscore(wparam, 'WT')
        if (len(outs) != 0): 
            outlist = [] 
            for items in zip(outs, wtmp):
                tmpout = []
                for item in items:
                    tmpout += item
                outlist.append(tmpout)
            outs = outlist
        else: outs = wtmp         
    if len(bparam) != 0:
        btmp = who.cal_zscore(bparam, 'BMI')
        if (len(outs) != 0): 
            outlist = []
            for items in zip(outs, btmp):
                tmpout = []
                for item in items:
                    tmpout += item
                outlist.append(tmpout)
            outs = outlist
        else: outs = btmp 
    if len(hcparam) != 0:
        hctmp = who.cal_zscore(hcparam, 'HC')
        if (len(outs) != 0): 
            outlist = []
            for items in zip(outs, hctmp):
                tmpout = []
                for item in items:
                    tmpout += item
                outlist.append(tmpout)
            outs = outlist
        else: outs = hctmp 
    if len(hwparam) != 0:
        hwtmp = who.cal_zscore(hwparam, 'HW')
        if (len(outs) != 0): 
            outlist = []
            for items in zip(outs, hwtmp):
                tmpout = []
                for item in items:
                    tmpout += item
                outlist.append(tmpout)
            outs = outlist
        else: outs = hwtmp  
  
    #for item in outs: 
    #    tmpouts.append(item)
    tmpouts += outs 
    
#def main(infile, gender, mode, cdcfile, whofile, whofile_hw):
def zscore_cal(infile, gender, mode, cdcfile, whofile, whofile_hw):

    #path = '/Users/pangx/Documents/data/project1/codes/'
    #path = ''
    #input = [person_id, dob, m_date, bmiz, height, weight]
    #outfile = infile + '_out_total.csv'

    #infile = path + infile + '.csv'
    mode = mode.upper()
    global tmpouts; tmpouts = []
    
    '''
    input = []
    with open(infile, 'r') as f: 
        for line in f: 
            elm = line.split()
            input.append(elm)
     '''   
    if (gender == 'F'):
        gender = 2
    elif (gender == 'M'):
        gender = 1
    else: 
        print("Please enter: input file, gender, calculation_mode, cdc_ref file, who_ref file, who_ref_wh file")
        exit()
    if mode not in ['C', 'HT', 'WT', 'HW1', 'HW2', 'HWC1', "HWC2"]:
        print("Wrong calculations mode. Must be: \n 0. C for head circumference \n 1. HT for height, \n 2. WT for weight, \n \
        3. HW1 for height/weight/bmi, \n 4. HW2 for height/weight/bmi/weight_height, \n \
        5. HWC1 for height/weight/bmi/headcircumference, \n 6. HWC2 height/weight/bmi/height_weight/headcircumference")
        exit()
    
    intitle,  outtitle = ['person_id', 'agemos'],  ['person_id', 'agemos']
    outinfo = "Performing"
    
    if mode.find('H') != -1:
        intitle += ['hagemos', 'height']
        outtitle += ['hagemos', 'height', 'hz', 'hp', 'hf', 'hpct95',  'hbiv']
        outinfo += " Height"
    if mode.find('W') != -1:
        intitle += ['wagemos', 'weight']
        outtitle += ['wagemos', 'weight', 'wz', 'wp', 'wf', 'wpct95', 'wbiv']
        outinfo += " Weight"
    if mode.find('W') != -1 and mode.find('H') != -1:
        outtitle += ['wagemos', 'bmi', 'bz', 'bp', 'bf', 'bpct95',  'bbiv']
        outinfo += " BMI"
    if mode.find('C') != -1:
        intitle += ['hcagemos', 'headc']
        outtitle += ['hcagemos', 'headc','cz', 'cp', 'cf', 'cpct95','cbiv']
        outinfo += " HeadC"
    if mode.find('2') != -1:
        outtitle += ['height', 'weight', 'hwz', 'hwp', 'hwf', 'hwpct95','hwbiv']
        outinfo += " Weight_height"
    print(outinfo + " calculations...")
    
    INPUT= infile #pd.read_csv(infile, skiprows = 0, delimiter = ',')
    INPUT = INPUT.sort_values(['agemos'])    #print(INPUT)
    
    colist = INPUT.columns.tolist()
    global intitledic 
    intitledic = dict((i, j) for j, i in enumerate(intitle) )
    columndic = dict((i, j) for j, i in enumerate(colist))
    
    allinput = INPUT.values.tolist()    #print(input)
    print(len(allinput), " number of measurements were read in")

    
    whoinput = []
    cdcinput = []
    tmp = []
    
    #for i in intitle:
        
    for element in allinput:     
        tmp = [element[columndic[i]] for i in intitle]
        if(element[columndic['agemos']] <= 24):
            whoinput.append(tmp)
            #print(tmp[1])
        else:   #if(element[columndic['wagemos']] > 24):
            cdcinput.append(tmp)   
            
             
    if (len(cdcinput) >0 ):
        cdc_run(cdcinput, gender, cdcfile, mode)       
        print(len(tmpouts))     
    if(len(whoinput) >0 ):
        who_run(whoinput, gender, whofile, whofile_hw, mode)
        print(len(tmpouts))     



    '''
    with open(cdcin, 'w') as f:
        wr = csv.writer(f)
        wr.writerow(cdctitle)
        for row in cdcinput: 
            wr.writerow( row + [gender])
            #print(row)
            
        
    with open(whoin, 'w') as f:
        wr = csv.writer(f)
        wr.writerow(whotitle)
        for row in whoinput: 
            wr.writerow(row + [gender])
    '''
    
    tmpouts.sort(key=operator.itemgetter(0, 1), reverse=False)
    ''' 
    with open(outfile, 'w') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(outtitle)
        wr.writerows(tmpouts)
    print("done!")
    '''

    outputs_df = pd.DataFrame.from_records(tmpouts, columns = outtitle)
    return outputs_df

def main():
    INPUT= pd.read_csv("/Users/pangx/Documents/data/project1/codes/hwbmi_8507_random200.csv", skiprows = 0, delimiter = ',')
    output = zscore_cal(INPUT, 'M', 'HW2', '/Users/pangx/Documents/eclipse_workplace/pbd_test/zscore_convertor/cdcref_d_pxq', \
                        '/Users/pangx/Documents/eclipse_workplace/pbd_test/zscore_convertor/WHOref_d_pxq', \
                        '/Users/pangx/Documents/eclipse_workplace/pbd_test/zscore_convertor/WHOref_d_hw_pxq' )
    output.to_csv("./hwbmi_8507_random200_zscore.csv")
    '''
    INPUT= pd.read_csv("/Users/pangx/Documents/data/project1/codes/headc_8507_random200.csv", skiprows = 0, delimiter = ',')
    output = zscore_cal(INPUT, 'M', 'C', '/Users/pangx/Documents/eclipse_workplace/pbd_test/zscore_convertor/cdcref_d_pxq', \
                        '/Users/pangx/Documents/eclipse_workplace/pbd_test/zscore_convertor/WHOref_d_pxq', \
                        '/Users/pangx/Documents/eclipse_workplace/pbd_test/zscore_convertor/WHOref_d_hw_pxq' )
    output.to_csv("./headc_8507_random200_zscore.csv")
    '''    
    print("Done")
    
if __name__ == '__main__':
    main()
#    zscore_cal('./test3', 'M', 'Hw2', './cdcref_d_pxq', './WHOref_d_pxq', './WHOref_d_hw_pxq')  
    


