'''
Created on Sep 18, 2017

@author: pangx
'''
import pandas as pd
import math
import scipy.stats as st
import numpy as np
import pandas as pd
from _pytest.compat import NoneType
from math import nan

##should put into a param list
CWAL = -5; CWAH = 8; CHAL = -5; CHAH = 4; CBMIL = -4; CBMIH = 8; CHCL = -5; CHCH = 5; CHWL= -4; CHWH = 8

class CDCCalculator(): 
    #def cal_zscore(self, val, l, m, s, z, p, f):
    def cal_zscore(self, param, label):
        outs = []
        
        for i in range(0, len(param) ) :
            tmp = param[i]
            #age = self.age_stat(param[i][-2])
            age = param[i][-2]
            val = param[i][-1]
            if np.isnan(param[i][:6]).any():
                outs.append( [ age, val, math.nan, math.nan, math.nan, math.nan, math.nan])
                continue
            

            params = self.lms_cal(age, param[i])
            l = params[0]; m = params[1]; s = params[2]
            
            if  val <= 0: 
                print("The value: height, weight, or headc must be positive")
                print(param[i], label)
                exit(-1)
            #elif (val == 0):
            #    z = f = -50
            else:             
                if abs(l) >= 0.01:
                    z = (pow((val / m), l) - 1) / (l * s)
                elif (l != None) & (abs(l) < 0.01):
                    z = math.log(val/m) / s
    
                if (val < m ) :
                    sdl = (m - m*pow((1 - 2 * l * s), (1/l)))/2
                    f = (val - m)/sdl
                else: 
                    sdh = (m * pow((1 + 2 * l *s), (1/l)) - m)/2
                    f = (val - m)/sdh                
                    
            p = 100 * st.norm.cdf(z)   
                       
            q95 = m * (( 1 + l * s * st.norm.ppf(0.95)) ** (1/l))
            qpct95 = 100 *(val/q95)
            
            biv = 0
            
            if  (label == 'WT'):
                if ( f <= (CWAL -2) ): biv = -3
                elif ( f <= (CWAL - 1) ): biv = -2
                elif ( f <= CWAL ): biv = -1
                elif ( f >= (CWAH + 2) ): biv = 3
                elif ( f >= (CWAH + 1) ): biv = 2
                elif ( f >= CWAH ): biv = 1            
                    
            elif  (label == 'HT'):
                if ( f <= (CHAL -2) ): biv = -3
                elif ( f <= (CHAL - 1) ): biv = -2
                elif ( f <= CHAL ): biv = -1
                elif ( f >= (CHAH + 2) ): biv = 3
                elif ( f >= (CHAH + 1) ): biv = 2
                elif ( f >= CHAH ): biv = 1
                         
            elif  (label == 'BMI'):
                if ( f <= (CBMIL -2) ): biv = -3
                elif ( f <= (CBMIL - 1) ): biv = -2
                elif ( f <= CBMIL ): biv = -1
                elif ( f >= (CBMIH + 2 )): biv = 3
                elif ( f >= (CBMIH + 1) ): biv = 2
                elif ( f >= CBMIH ): biv = 1
                
            elif  (label == 'HC'):
                if ( f <= (CHCL -2) ): biv = -3
                elif ( f <= (CHCL - 1) ): biv = -2
                elif ( f <= CHCL ): biv = -1
                elif ( f >= (CHCH + 2 )): biv = 3
                elif ( f >= (CHCH + 1) ): biv = 2
                elif ( f >= CHCH ): biv = 1
                
            elif  (label == 'HW'):
                if ( f <= (CBMIL -2) ): biv = -3
                elif ( f <= (CBMIL - 1) ): biv = -2
                elif ( f <= CBMIL ): biv = -1
                elif ( f >= (CBMIH + 2 )): biv = 3
                elif ( f >= (CBMIH + 1) ): biv = 2
                elif ( f >= CBMIH ): biv = 1

            outs.append( [age, val, z, p, f, qpct95, biv])
        return outs

##may not need here
    def age_stat(self, agemon):
        if (agemon > 0 ) & (agemon < 0.5):
            agecat = 0
        elif agemon != None:
            agecat = int(agemon + 0.5) - 0.5
        return agecat
          
    def bmi_cal(self, height, weight, agemon):
        bmi = 0.0
        heightcat = self.height_stat(height)
        #agecat = self.age_stat(agemon)
        if ((agemon >= 24) & (weight > 0) & (height > 0)):
            bmi = (float  (weight)) / pow((height/100), 2)  
        if bmi == 0: 
            print(weight, height, heightcat, agemon, bmi) 
        return bmi
            
    def height_stat(self, height):
        if (height >= 45.5):
            heightcat = int (height +0.5) - 0.5
        elif (height >= 45):
            heightcat = 45
        else: heightcat = np.NaN
        return heightcat
            
####not needed in who calculations, because age is in days; in cdc parameters are estimated to from those two cloest months
    def lms_cal(self, age,  param):
        #ageint = param.loc(param['_AGEMOS2']) - param.loc(param['_AGEMOS1'])
        #print(param)
        ageint = 1
        deltage = age - param[6]
        calparam = []
        
        #print()
        for i in range(3):
            cal = param[i] + deltage * (param[i+3] - param[i]) /ageint
            calparam += [cal]
            
        #print(param0)
        return calparam
