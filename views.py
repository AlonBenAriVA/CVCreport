from django.shortcuts import render
from django.http import HttpResponse
from django.db import connection
import pandas as pd
import json
import os
import pyodbc
from operator import itemgetter
import numpy as np



def index(request):
   
    return render(request, 'index.html',{})

# Create your views here.
def report(request):
    """
    A view to return the number of linedays for each month
    """
    cur = connection.cursor() # a connection is made and a query string is passed below
    try:
        cur.execute('select PatientSSN, PatientName,Sta3n, Age, Gender, WardLocationName,RoomBed, HealthFactorType, AdmitDateTime, HealthFactorDateTime, SpecimenTakenDateTime, DischargeDateTime,RequestingWard,SpecimenComment,CollectionSample ,OrganismQuantity, AntibioticSensitivityValue, AntibioticSensitivityComments, Antibiotic, DrugNodeIEN, AntibioticDisplayComment, LabProcedure, Organism,OrganismCategory, GramStain ,PatientCity, PatientCounty,PatientState,PatientZip,PatientZip4,PatientLON,PatientLAT,PatientFIPS,PatientMarket,PatientSubmarket,Growth ,Inpatient, LineNew, LineStatus, LineLoc,LineRemoved from LSV.MAC_CVC')
        data = cur.fetchall()
        cvc = Filter(data)  # instantiating the Filter class (below)
        report = cvc.make_report()  # report hold a json object that is sent to the view below
    finally:
        cur.close()
    
    return render(request, 'report.html',{'report':report})


def get_sql(request):
    cur = connection.cursor()
    try:
        # cur.execute('select PatientSSN, PatientName,Sta3n, Age, Gender, WardLocationName,RoomBed, HealthFactorType, AdmitDateTime, HealthFactorDateTime, SpecimenTakenDateTime, DischargeDateTime,RequestingWard,SpecimenComment,CollectionSample ,OrganismQuantity, AntibioticSensitivityValue, AntibioticSensitivityComments, Antibiotic, DrugNodeIEN, AntibioticDisplayComment, LabProcedure, Organism,OrganismCategory, GramStain ,PatientCity, PatientCounty,PatientState,PatientZip,PatientZip4,PatientLON,PatientLAT,PatientFIPS,PatientMarket,PatientSubmarket,Growth ,Inpatient, LineNew, LineStatus, LineLoc,LineRemoved from LSV.MAC_CVC')
        # data = cur.fetchall()
        data = DBG.debug()
        cvc = Filter(data)
       
     
        cvc_data = []
        for i,ssn in enumerate(cvc.patient_ssn):
            cvc_data.extend([cvc.get_vis(i,ssn,cvc.patient_history[ssn])])
        cvc_data =  sorted(cvc_data,key =itemgetter('admit_date'),reverse=True)
        jn_dict = {i:p for i,p in enumerate(cvc_data)}
        
    finally:
        # cur.close()
        print('done')
    return render(request, 'data.html',{'d':cvc_data})

#######class to help render the data in the correct format.
class DBG:
     def debug(server ='VHACDWDWHSQL33.vha.med.va.gov' ,db = 'D05_VISN21Sites' ):
        '''
        A debug utility  to run from shell
        '''
        # sql = 'select * from LSV.MAC_CVC' 
        sql = 'select * from LSV.MAC_CLI'
        
        try:
            conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes;Integrated Security=SSPI')
            data = pd.read_sql(sql = sql,con = conn,params=[])
            print(data.shape)
        finally:
            conn.close()
        
        return data

class Filter():
    def __init__(self,data):
        '''
        Initializa dataset and format timestamps
        '''
       
        self.data = pd.DataFrame(data)
        # self.data.columns = [ 'PatientSSN','PatientName','Sta3n', 'Age', 'Gender', 'WardLocationName','RoomBed', 
        #                         'HealthFactorType','AdmitDateTime','HealthFactorDateTime', 'SpecimenTakenDateTime', 'DischargeDateTime', 'RequestingWard', 
        #                         'SpecimenComment', 'CollectionSample' ,'OrganismQuantity', 'AntibioticSensitivityValue',
        #                          'AntibioticSensitivityComments', 'Antibiotic', 'DrugNodeIEN',
        #                          'AntibioticDisplayComment', 'LabProcedure', 'Organism', 'OrganismCategory', 'GramStain', 'PatientCity', 'PatientCounty',
        #                          'PatientState','PatientZip','PatientZip4','PatientLON','PatientLAT','PatientFIPS','PatientMarket','PatientSubmarket',
        #                          'Growth','Inpatient','LineNew','LineStatus','LineLoc','LineRemoved']
        
        # self.ssn_dict = {self.data.iloc[p,:]['PatientSSN']:self.data.iloc[p,:]['PatientName']  for p in self.data.index}
        self.ssn_dict = {ssn:data[data['PatientSSN']==ssn].PatientName.unique()[0] for ssn in data.PatientSSN.unique()}
        self.data['HealthFactorDate'] =  self.data.HealthFactorDateTime.apply(pd.to_datetime).dt.date  # get only the month out.

        self.data.DischargeDateTime = self.data.DischargeDateTime.fillna(pd.Timestamp.today().strftime('%m-%d-%Y %H:%M:%S')) # fix NAN in discharge dates (death, still inpat etc.)

        self.patient_ssn = self.data.PatientSSN.unique() # get unique SSNs

        self.patient_history = {p:self.data[self.data.PatientSSN == p] for p in self.patient_ssn } # get the relevant patient chunk of data from table

        self.patient_admit = { p: self.patient_history[p].AdmitDateTime.unique() for p in self.patient_ssn } #dictionary of patient admits
        self.patient_discharge = { p: self.patient_history[p].DischargeDateTime.unique() for p in self.patient_ssn } # dictionary of patient discharge
        self.floors = self.floor_set()

    


    def floor_set(self):
        '''
        A method to return a set of the floors available
        '''
        set0 = set(self.data['AdmitWardLocationName'])
        set0.update(self.data['DischargeWardLocationName'])
        return {w: {m:0 for m in range(1,13)} for w in set0 }
        

    def array2dict(self,tuple):
        '''
        A utility to  create a dictionary of months and counts from two array
        Parameters:
        dict: a ditctionary {ward, (array(months), array(counts))} from get_linedays
        '''
        
        return {k:v for k,v in zip(tuple[0],tuple[1])}

    def sum_linedays(self,ssn):
        '''
        A method to summarize the number of linedays per each floor per patient by ssn
        '''
        ldays = self.get_linedays(ssn)
        
        return   [{[k for k in k.keys()][0]:self.array2dict([r for r in k.values()][0])} for k in [d for d in ldays]]      


    def tally_linedays(self):
        '''
        A method to tally all linedays
        '''
        itemized = [ld for ld in map(lambda x: self.get_linedays(x)  ,self.patient_ssn)] #  for each SSN the line days for each floor.
        ##
        # populate self.floors
        #
        return itemized

    def get_linedays(self,ssn):
        '''
        A method to return the line days for each ward the patient was at 
        [{ward:(months, lindays per month)}]
        '''
        linedays =[]
        d = self.get_location(ssn, self.patient_history[ssn]) # get the location list of the patient
        if (d):
            for w  in d:
                mask = (self.patient_history[ssn].HealthFactorDateTime > pd.to_datetime(w['start'])) & ( self.patient_history[ssn].HealthFactorDateTime <pd.to_datetime( w['end']))
                linedays.append( {
                    w['content']:
                    np.unique(np.array([i for i in map(lambda x: x.month ,self.patient_history[ssn][mask].HealthFactorDate.unique())]),return_counts=True)
                    })
        return {ssn:linedays}

    def get_location(self,ssn, patient_history):
        '''
        A method to return a dictionary of WardLocation and dates for that location
        for each patient.
        '''
        ward_locs = []
        patient = self.patient_history[ssn]
        course =  patient[['AdmitWardLocationName','AdmitDateTime','DischargeWardLocationName','DischargeDateTime']].drop_duplicates().dropna().sort_values(by = 'AdmitDateTime').to_dict(orient = 'records')
        for c in course:
                print(c['AdmitWardLocationName'])#, c['AdmiteDateTime'], c['DischargeDateTime'])
                ward_locs = [ 
                    {'content':c['AdmitWardLocationName'],'start':c['AdmitDateTime'].strftime('%Y-%m-%d'),'end':c['DischargeDateTime'].strftime('%Y-%m-%d')} for c in course
                    ] 
                print(c['AdmitWardLocationName'], c['AdmitDateTime'], c['DischargeDateTime'],ssn) 
        return ward_locs

        


    def get_maint(self, pt_hx):
        """
        A method to return a data frame of health factors relating to maintenance
        Input- 
        patient history
        """
        loc_ix = [i for i,j in enumerate(pt_hx.HealthFactorType) if 'STATUS' or 'NEW' in j]
        maint_hx =  pt_hx.iloc[loc_ix][['HealthFactorType','HealthFactorDateTime','HealthFactorDate']]
        return maint_hx

    def get_events(self, ssn):
        """
        A method to return events  WITHIN in patient period and outside of that period
        """
        indexes = self.patient_history[ssn].index.tolist()
        in_index =[]
        dict_inpat= {}
        dict_outpat = {}
        dict_outpat['non_inpat']=[]
        for i in range(len(self.patient_admit[ssn])):
            mask_inpat = ( self.patient_history[ssn].HealthFactorDateTime >= self.patient_admit[ssn][i]) & (self.patient_history[ssn].HealthFactorDateTime <= self.patient_discharge[ssn][i])
            in_index.extend(self.patient_history[ssn].loc[mask_inpat.values].index.tolist())
            #
            dict_inpat[(self.patient_admit[ssn][i],self.patient_discharge[ssn][i])] = self.patient_history[ssn].loc[mask_inpat.values]
            #mask_outpat = ( patient_history[ssn].HealthFactorDateTime <= patient_admit[ssn][i]) & (patient_history[ssn].HealthFactorDateTime >= patient_discharge[ssn][i])
            
        if (len(in_index)==0):
            print('no inpat records')
            dict_outpat['non_inpat'] = self.patient_history[ssn]
        else:
            for k in in_index:
                indexes.remove(k)
            dict_outpat['non_inpat'] = self.patient_history[ssn].loc[indexes]
        return {'inpat':dict_inpat,'outpat':dict_outpat}


    def get_bugs(self,ssn):
        """
        return the type  and dates of samples taken
        """
        return self.patient_history[ssn][['SpecimenTakenDateTime','Organism','GramStain','SpecimenComment']].dropna().drop_duplicates()


    def get_stats(self,ssn):
        """
        A method to get stats from get_events() return a dictionary {} with the follwoing line items:
        return number days maintained as inpatient/outpatient.
        """   
        maint = {}
        maint['inpat_maint'] = 0
        maint['outpat_maint'] = 0
        # iterate over keys
        try:
            keys_events = [k for k in self.get_events(ssn)['inpat'].keys()]# get keys:

            print({k:self.get_maint(self.get_events(ssn)['inpat'][k]).HealthFactorDateTime
                            .apply(pd.to_datetime)
                            .dt
                            .date.unique().shape[0] for k in keys_events })

            maint['inpat_maint'] = {k:self.get_maint(self.get_events(ssn)['inpat'][k]).HealthFactorDateTime
                                    .apply(pd.to_datetime)
                                    .dt
                                    .date.unique().shape[0] for k in keys_events }
            
        except:
            print('no Inpat logs')

        try:
            print(self.get_maint(self.get_events(ssn)['outpat']['non_inpat']).HealthFactorDateTime.apply(pd.to_datetime).dt.date.unique().shape[0]
            )
            maint['outpat_maint'] =self.get_maint(self.get_events(ssn)['outpat']['non_inpat']).HealthFactorDateTime.apply(pd.to_datetime).dt.date.unique().shape[0]
            
        except:
            print('No outpat maint notes')
        return {ssn:{'maint':maint}}


    def get_summary(self):
        """
        A method to return lines days and BUGS
        """
        return {a:{'linedays':self.get_stats(a),'infection':self.get_bugs(a)} for a in self.patient_ssn}
        

    def maint_start_end(self, maint_hx):
        """
        A method to return the min and max of maintenance data framedays.
        """
        start = maint_hx.HealthFactorDateTime.min()
        stop = maint_hx.HealthFactorDateTime.max()
        m = maint_hx[(maint_hx.HealthFactorDateTime >= start) & (maint_hx.HealthFactorDateTime <= stop)]
        line_days = maint_hx.HealthFactorDate.unique().shape[0]
        return m,start,stop,line_days
        
    
    def get_vis(self,i,ssn,patient_history):
        """
        Output a json data object for vis.js
        id - id number
        ssn -  ssn of patient
        """
        
        jsn = {}
        m, start, end, line_days = self.maint_start_end(self.get_maint(self.patient_history[ssn]))
        
        jsn['id'] = str(i)
        jsn['ssn'] = ssn
        jsn['PatientName'] = self.ssn_dict[ssn]
        jsn['linedays_id'] = 'linesdays'+str(i)
        jsn['bugs_id'] = 'bugs'+str(i)
        jsn['admit_id'] = 'admit'+str(i)
        jsn['admit_date'] = [str(d).split('T')[0] for d in self.patient_admit[ssn]]
        jsn['content'] = 'line loc'
        jsn['start'] = str(start)
        jsn['end'] = str(end)
        jsn['line_days'] = line_days
        try:
            jsn['ward'] = self.get_location(ssn,self.patient_history[ssn])
        except:
            print( '%s,Missing pieces of data',ssn)
        jsn['table'] = m.drop_duplicates().to_json(orient = 'records', date_format = 'iso')
        if (self.get_bugs(ssn).shape[0] != 0):
            jsn['bugs'] = {'a':self.get_bugs(ssn).drop_duplicates().to_html(index=False)} #json(orient = 'records', date_format = 'iso')}
            jsn['bugs_json'] = self.get_bugs(ssn).drop_duplicates().to_json(orient = 'records', date_format = 'iso')
            
        else:
            jsn['bugs'] = {'a':''}
            jsn['bugs_json'] = 0
        # 
        return jsn   

    def make_report (self):
        """
        A method to return a summary report of line days per month and infections
        """
        report = {}
        df = pd.DataFrame()
  
        for ssn in self.patient_ssn:
            hx = self.data[self.data.PatientSSN == ssn].to_dict('records')
            days = list(set([(status['HealthFactorDate'], 1,status['WardLocationName'])  for
                        status in hx if status['HealthFactorType'].find('NEW') !=-1 or status
                        ['HealthFactorType'].find('STATUS') !=-1]))
            
            report[ssn] = days
        # for k in report.keys():
        #      df = df.append(pd.DataFrame(report[k], columns = ['date','line_days']),ignore_index =True)
        #      df['month'] =  df.date.apply(lambda x:  pd.to_datetime(x).month)
            
        return report #df.groupby('month','WardLocationName').sum().to_dict('index')