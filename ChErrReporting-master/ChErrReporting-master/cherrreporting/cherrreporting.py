# -*- coding: utf-8 -*-
#!/usr/bin/env python
# coding: utf-8

# Importing libraries
import pandas as pd
import db as db # For DB connection
import sendmail as mail
import json
import requests

#for maps and plots
import matplotlib.pyplot as plt
from datetime import date, timedelta, datetime
#import Scheduler library
import schedule
import time


class cherrreporting():
    def post_to_es(self, df, report_dt):
        # df is a dataframe or dataframe chunk coming from your reading logic
        #df['_id'] = df['CLM_SYS_ID'] + '_' + df['column_2'] # or whatever makes your _id
        df['_id'] = df['CLM_SYS_ID']+ '_'+df['ERR_CD']
#        df['ERR_DT'] = df['ERR_DT'].apply(lambda x: x.strftime('%Y%m%d'))
        df_as_json = df.to_json(orient='records', lines=True, date_format='iso')
#        print(df_as_json)
        final_json_string = ''
        for json_document in df_as_json.split('\n'):
            jdict = json.loads(json_document)
            metadata = json.dumps({'index': {'_id': jdict['_id']}})
            jdict.pop('_id')
            final_json_string += metadata + '\n' + json.dumps(jdict) + '\n'
        
        print (final_json_string)
        
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        es_url = 'http://elasticsearch.chwy.optum.com/cherrrpt-' + str(report_dt) + '/index/_bulk'
        #es_url = 'http://elasticsearch.chwy.optum.com/cherrrpt/index/_bulk'
        r = requests.post(es_url, data=final_json_string, headers=headers, timeout=60) 
        print (r.text)
    
    def gen_report(self, report_dt):
        print ("Starting the report generation...")
#        report_dt = date.today() - timedelta(1)
        print ("Preparing report for :", str(report_dt))
        # Create DB Connection
    #    conn = db.make_dbconn('DB0P', 'gwya0002','50000','TCPIP','TSUF2ZY','JUN20JUN')
        conn = db.make_dbconn('DB0P', 'gwya0002','50000','TCPIP','ML5687U1','X$PW83EW')
        
        # Prepare CH_ERR_RPT sql stmt
        
        sql = 'select date(A.LST_UPDT_DTTM) AS ERR_DT, A.INVN_CTL_NBR,A.ERR_CD,A.CLM_SYS_ID,A.ERR_LOGC_DEL_IND,B.ERR_CD_DESC from d5687chy.ch_err_rpt A, D5687CHY.CH_ERR_CD_DESC B where A.ERR_CD = B.ERR_CD and date(A.lst_updt_dttm) = ? and A.ERR_LOGC_DEL_IND = ? with ur'
        err_dt = str( report_dt)
        err_logc_del_ind = "N"
        total_data = db.get_data(conn,sql,err_dt,err_logc_del_ind)
        
        df = pd.DataFrame.from_dict(total_data,orient='index')
        #df.head(2)
        
        group_errcd_sys = df.groupby(['ERR_CD','CLM_SYS_ID'])['ERR_LOGC_DEL_IND'].count()
        group_errcd_sys1 = df.groupby(['ERR_DT','ERR_CD','CLM_SYS_ID','ERR_CD_DESC'])['ERR_LOGC_DEL_IND'].count()
        group_errcd_sys.head()
        
        group_sys = df.groupby(['CLM_SYS_ID'])['INVN_CTL_NBR'].count()
        group_sys.head()
        
        group_errcd_sys_data = group_errcd_sys1.to_frame().reset_index()
        group_sys_data = group_sys.to_frame().reset_index()
        
        # email data
        email_errcd_sys_data = group_errcd_sys_data.sort_values(by=['ERR_LOGC_DEL_IND'],ascending=False).reset_index(drop=True).head(10)
        email_errcd_sys_data = email_errcd_sys_data.rename(columns = {"ERR_LOGC_DEL_IND":"Counts"})
        
        # es data
        es_errcd_sys_data = group_errcd_sys_data.sort_values(by=['ERR_LOGC_DEL_IND'],ascending=False).reset_index(drop=True)
        es_errcd_sys_data = es_errcd_sys_data.rename(columns = {"ERR_LOGC_DEL_IND":"Counts"})
        
        
        email_sys_data = group_sys_data.sort_values(by=['INVN_CTL_NBR'],ascending=False).reset_index(drop=True).head()
        email_sys_data = email_sys_data.rename(columns={"INVN_CTL_NBR":"Counts"})
        print ("Data is fetched. Generating plots now...")
        
        #plot data
        fig, ax = plt.subplots(figsize=(14,15))
        group_errcd_sys.sort_values(ascending=False).plot(title="Accum Errors plot",kind='bar')
        #plt.show()
        plt.savefig("./plots/"+err_dt+"-plot.png")
        plt.close()
        
        fig, ax = plt.subplots(figsize=(9,9))
        group_sys.sort_values(ascending=False).plot(title="CAE wise Errors plot",kind='bar')
        #plt.show()
        plt.savefig("./plots/"+err_dt+"-caeplot.png")
        plt.close()
        
        #Send email
        mail.sendmail(email_errcd_sys_data, err_dt)        
        
        #Exit
        print("Error report generation is complete for today.")
        #exit()
    
#        email_errcd_sys_data['ErrDate'] = report_dt
        print (es_errcd_sys_data.head())
        return es_errcd_sys_data
#        self.post_to_es(email_errcd_sys_data, report_dt)
    
    def gen_rer_data(self, report_dt):
        
        # Create DB Connection
        conn = db.make_dbconn('DB0P', 'gwya0002','50000','TCPIP','ML5687U1','X$PW83EW')
                
        # Prepare RER sql stmt
        rer_sql = "select date(lst_updt_dttm) as RER_DT,  \
              proc_trans_typ_cd as TRAN, \
              count(distinct invn_ctl_nbr) as err_counts \
            from d5687chy.ch_err_Rpt \
            where date(lst_updt_Dttm) = ? \
              group by date(lst_updt_dttm), proc_trans_typ_cd \
            with ur "
        err_dt = str(report_dt)
        rer_data = db.get_rer_data(conn, rer_sql, err_dt)
        
        rer_df = pd.DataFrame.from_dict(rer_data,orient='index')
        rer_df['REC_CTGY'] = rer_df['TRAN'].apply(lambda x: self.rer_rec_ctgy(x))
        print (rer_df.head())
        
    def rer_rec_ctgy(self, x):
        accum_type = ['ACCINT','ACCACK','ACCBAL','ACCUPD','ACCMAINT']
        adj_type = ['ADJCLM','EDJCLM','ADJP2']
        if str(x).strip() in accum_type:
            return "ACCUM"
        elif str(x).strip() == 'CHFINAL':
            return "MNR-EOB"
        elif str(x).strip() in adj_type:
            return "ADJUDICATION"
        else:
            return 'UNKN_CTGY'


if __name__ == '__main__':
    
    print (str(datetime.now()), "Initiating Scheduler")
    def job():
        genrpt = cherrreporting()
        report_dt = date.today() - timedelta(1)
        es_data = genrpt.gen_report(report_dt)
        genrpt.post_to_es(es_data, report_dt)
#        rer_data = genrpt.gen_rer_data(report_dt)
#        print (rer_data)
#    job()
#    schedule.every(1).minutes.do(job)
    schedule.every().day.at("07:00").do(job)
    while True:
        print (str(datetime.now()), "Going to run pending")
        schedule.run_pending()
        time.sleep(300)