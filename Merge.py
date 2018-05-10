
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np


# In[1]:


log_folder = '../MetaTutor Data/Log Files/Day 2/'
log_files = ['MT430PN56165.log','MT430PN56166.log','MT430PN56167.log','MT430PN56168.log']
logfile_cols = ["Line","Time","Time(ms)","Layout","Agent","Action","Detail","Phonetic Correction"]
facet_folder = '../MetaTutor Data/FACET Files (NCSU Only)/'
facet_files = ['165_FACET.txt','166_FACET.txt','167_FACET.txt','168_FACET.txt']


# In[3]:


def get_log_file_offset(filename,match_string):
    #Read till you get 'Day 2', present in every log file
    f = open(filename,'r')
    count=0
    for line in f.readlines():
        count+=1
        if line.startswith(match_string):
            break
    f.close()
    return count


# In[ ]:


for i in range(1,4):
    log_file = log_files[i]
    facet_file=facet_files[i]
    curfile = log_folder+log_file
    log_data = pd.read_csv(curfile,sep='\t',skiprows=get_log_file_offset(curfile,'Day'),names = logfile_cols)
    log_data['Time Elapsed(ms)'] = log_data['Time(ms)'].copy()
    log_data['Time(ms)'] = log_data['Time(ms)'].apply(lambda x: x%1000)
    log_data['Timestamp'] = (pd.to_datetime('2015-02-04 '+log_data['Time'],format='%Y-%m-%d %H:%M:%S').astype(np.int64)//10**6)+(log_data['Time(ms)'])
    log_events = log_data[log_data.Detail.isin(['Start','Stop'])]
    facet = pd.read_csv(facet_folder+facet_file,sep='\t',skiprows=get_log_file_offset(facet_folder+facet_file,'StudyName')-1)
    facet_agent_action_column = np.full(facet.shape[0],'')
    facet_action_detail_column = np.full(facet.shape[0],'')
    facet['Timestamp'] = pd.to_datetime(facet.Timestamp,format='%Y%m%d_%H%M%S%f').astype(np.int64)//10**6
    log_events2 = log_events.reset_index()
    log_iterator = 0
    facet_iterator = 0
    log_column = list(log_events2.columns).index('Action')
    while facet_iterator<facet.shape[0]:
        if log_iterator>=log_events2.shape[0]:
            print("Log Events End Reached")
	    break
	else:
            if facet.Timestamp[facet_iterator]<log_events2.Timestamp[log_iterator]:
                facet_iterator+=1
                continue
            elif facet.Timestamp[facet_iterator]>=log_events2.Timestamp[log_iterator]:
                facet_action_detail_column[facet_iterator] = log_events2.iloc[log_iterator,log_column+1] 
                log_iterator+=1
                while facet.Timestamp[facet_iterator]<log_events2.Timestamp[log_iterator]:
                    #facet.iloc[facet_iterator,facet_column] = log_events2.iloc[log_iterator,log_column]
                    facet_agent_action_column[facet_iterator] = log_events2.iloc[log_iterator,log_column]               
                    facet_iterator+=1
                facet_action_detail_column[facet_iterator] = log_events2.iloc[log_iterator,log_column+1] 
                log_iterator+=1
            else:
                facet_iterator+=1

    facet['AgentAction'] = np.asarray(facet_agent_action_column)
    facet['ActionDetail'] = np.asarray(facet_action_detail_column)
    columns = list(facet.columns)
    columns = columns[:9]+columns[-2:]+columns[9:-2]
    facet.columns = columns
    facet.head()
    facet.columns
    facet.to_csv(facet_file+'.csv')


# In[6]:


'''The task is to get the exact time stamp from a combination of 'Time in seconds' and 'Time elapsed since start in ms'. 
For now, for the sake of simplicity, it is easy to just extract the ms from the data. The time processing can be done later on.'''

