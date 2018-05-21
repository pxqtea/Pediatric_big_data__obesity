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

##should put into a param list
WWAL = -6; WWAH = 5; WHAL = -6; WHAH = 6; WBMIL = -5; WBMIH = 5; WHCL = -5; WHCH = 5; WHWL= -5; WHWH = 5 ### WHO Z_value cutoffs

class WHOCalculator(): 
    #def cal_zscore(self, val, l, m, s, z, p, f):
    def cal_zscore(self, params, label):
        
        biv = 0
        mdf = 0
        outs = []
        
        for param in params: 
            val = param[-1]
            age = param[-2]
            if val <= 0: 
                print("height, weight, headc or bmi must be positive")
                exit(-1)
            if np.isnan(param[:3]).any():
                outs.append( [ age, val, math.nan, math.nan, math.nan, math.nan, math.nan])
                continue
            
            l = param[0]; m = param[1]; s = param[2]
           
            '''
           ### Who does not use the first adjustment 
            if abs(l) > 0.01:
                z = (pow((val / m), l) - 1) / (l * s)
            elif (l != None) & (abs(l) < 0.01):
                z = math.log(val/m) / s
            p = st.norm.cdf(z)
            '''
            z = (pow((val / m), l) - 1) / (l * s)
            f = z
            
            ### WHO adjustment for bmi and weight
            if (label == 'BMI' or label == 'WT') and (abs(z) > 3): 
                if ( z > 3):
                    sd2pos = m * (1+l*s*2)**(1/l)
                    sd3pos = m * (1+l*s*3)**(1/l)
                    sd23pos = sd3pos - sd2pos
                    f = 3 + (val - sd3pos)/sd23pos
                elif ( z < -3):
                    sd2neg = m * (1+l*s*(-2))**(1/l)
                    sd3neg = m * (1+l*s*(-3))**(1/l)
                    sd23neg = sd2neg - sd3neg
                    f = -3 + (val - sd3neg)/sd23neg
                
            p = 100 * st.norm.cdf(f)
            
            q95 = m * (( 1 + l * s * st.norm.ppf(0.95)) ** (1/l))
            qpct95 = 100 *(val/q95)
            ##qdif95 = val - q95
            q50 =  m * (( 1 + l * s * st.norm.ppf(0.50)) ** (1/l))
    
                ### apply cutoff to label biv values
            biv = 0
            if  (label == 'WT'):
                if ( f <= WWAL -2 ): biv = -3
                elif ( f <= WWAL - 1 ): biv = -2
                elif ( f <= WWAL ): biv = -1
                elif ( f >= WWAH + 2 ): biv = 3
                elif ( f >= WWAH + 1 ): biv = 2
                elif ( f >= WWAH ): biv = 1
            elif  (label == 'HT'):
                if ( f <= WHAL -2 ): biv = -3
                elif ( f <= WHAL - 1 ): biv = -2
                elif ( f <= WHAL ): biv = -1
                elif ( f >= WHAH + 2 ): biv = 3
                elif ( f >= WHAH + 1 ): biv = 2
                elif ( f >= WHAH ): biv = 1
            elif  (label == 'BMI'):
                if ( f <= WBMIL -2 ): biv = -3
                elif ( f <= WBMIL - 1 ): biv = -2
                elif ( f <= WBMIL ): biv = -1
                elif ( f >= WBMIH + 2 ): biv = 3
                elif ( f >= WBMIH + 1 ): biv = 2
                elif ( f >= WBMIH ): biv = 1
            elif  (label == 'HC'):
                if ( f <= WHCL -2 ): biv = -3
                elif ( f <= WHCL - 1 ): biv = -2
                elif ( f <= WHCL ): biv = -1
                elif ( f >= WHCH + 2 ): biv = 3
                elif ( f >= WHCH + 1 ): biv = 2
                elif ( f >= WHCH ): biv = 1
            elif  (label == 'HW'):
                if ( f <= WHWL -2 ): biv = -3
                elif ( f <= WHWL - 1 ): biv = -2
                elif ( f <= WHWL ): biv = -1
                elif ( f >= WHWH + 2 ): biv = 3
                elif ( f >= WHWH + 1 ): biv = 2
                elif ( f >= WHWH ): biv = 1
            if label == 'HW':
                outs.append( [age, val, z, p, f, qpct95, biv])
            else:
                outs.append( [age/30.4, val, z, p, f, qpct95, biv])
            if (biv <= -3): print("The is the update", age/30.4, z, f, biv)                        
        return outs
          
    def bmi_cal(self, height, weight):
        bmi = 0.0
        if ((weight > 0) & (height > 0)):
            bmi = weight / pow((height/100), 2)   
        return bmi
            
