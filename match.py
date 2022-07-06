import streamlit as st
import pandas as pd
import psycopg2 as pg
import collections, functools, operator
import numpy as np
engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")
df_touchpoints = pd.read_sql('select * from touchpoints', con=engine)
df_tags = pd.read_sql('select * from tags', con=engine)
df_tagging = pd.read_sql('select * from taggings', con=engine)
df_universities = pd.read_sql('select * from universities', con=engine)
df_cities = pd.read_sql('select * from cities', con=engine)
df_cities.rename(columns = {'name':'city_name'}, inplace = True)
df_subjects = pd.read_sql('select * from subjects', con=engine)
subject_topics = pd.read_sql('select * from subjects_topics', con=engine)
df_degrees = pd.read_sql('select * from degrees', con=engine)
df_tc = pd.read_sql('select * from touchpoints_cities', con=engine)
df_goals = pd.read_sql('select * from goals', con=engine)

df_goal_weights = pd.read_sql('select * from matching_goal_weights', con=engine)
grouped_1 = df_touchpoints.groupby(df_touchpoints.state)
df_touchpoints = grouped_1.get_group(1)
df_touchpoints =  pd.merge(df_touchpoints, df_tagging, left_on='id',right_on='taggable_id',suffixes=('', '_x'))
df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
df_touchpoints = pd.merge(df_touchpoints,df_tags,left_on='tag_id',right_on='id',suffixes=('', '_x'))
df = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
df = pd.merge(df,df_tc,left_on='id',right_on='touchpoint_id',suffixes=('', '_x'),how = 'left')
df = df.loc[:,~df.columns.duplicated()]
df_cities.rename(columns = {'name':'city_name'}, inplace = True)
df = pd.merge(df,df_cities,left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
df = df.loc[:,~df.columns.duplicated()]
df_universities = pd.merge(df_universities, df_cities, left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'inner')
df_universities = df_universities.loc[:,~df_universities.columns.duplicated()]
group_6 = df.groupby(df.type)
df_T = group_6.get_group("Topic")
df_goals = pd.merge(df_goals, df_goal_weights, left_on='id',right_on='goal_id',suffixes=('', '_x'),how = 'inner')
df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
df_goals = df_goals[['id','title','touchpointable_kind','value']].copy()
df_goals['value'] = df_goals['value'].div(10)



df_subjects = pd.merge(df_subjects, subject_topics, left_on='id',right_on='subject_id',suffixes=('', '_x'),how = 'inner')
df_subjects = df_subjects.loc[:,~df_subjects.columns.duplicated()]
df_subjects = pd.merge(df_subjects,df_tags,left_on='topic_id',right_on='id',suffixes=('', '_x'))
df_subjects = df_subjects.loc[:,~df_subjects.columns.duplicated()]
goals = ['Start my Career with a Spring Week','Get a Summer Internship','Get an Internship alongside my Studies', 'Land a Placement Year','Win Awards & Competitions','Secure a Graduate Job','Find a Co-founder & Start a Business', 'Meet Like-minded Students & join Societies','Expand my Network & Connect with Industry Leaders']

Goals =  st.multiselect('Enter the goals',goals,key = "one")
interest = st.multiselect('Enter the interest',df_T['name'].unique(),key = "two")
weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1,2,1]
Weight = st.multiselect('Enter the weight',weight,key = "three")
Interest = pd.DataFrame(interest,columns = ['Interest'])
Weight = pd.DataFrame(Weight,columns = ['Weight'])
df_interest = pd.concat([Interest,Weight],axis = 1)
University = st.selectbox('Enter the university',df_universities['name'].unique(),key = 'four')
Subject = st.selectbox('Enter the subject',df_subjects['name'].unique(),key = 'five')
Degree =  st.selectbox('Enter the degree',df_degrees['name'].unique(),key = 'six')
year = ['First Year ','Second Year','Third Year','Final Year']
Year = st.selectbox('Enter the year',year,key = 'seven')
submit_button = st.button('Submit',key = 'eight')
goals_1 =  pd.DataFrame(goals,columns =['Goals'])
df_goals = pd.merge(df_goals, goals_1, left_on='title',right_on='Goals',suffixes=('', '_x'),how = 'inner')
df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
df =  pd.merge(df, df_goals, left_on='kind',right_on='touchpointable_kind',suffixes=('', '_x'),how = 'inner')
df = df.loc[:,~df.columns.duplicated()]


if len(interest) > 0:
  group_7 = df.groupby(df.type)
  df_I = group_7.get_group("Topic")
  df_I =  pd.merge(df_I, df_interest, left_on='name',right_on='Interest',suffixes=('', '_x'),how = 'inner')
  df_I = df_I.loc[:,~df_I.columns.duplicated()]
  col_list = df_I['name'].unique()
  df_I['idx'] = df_I.groupby(['touchpointable_id', 'name']).cumcount()
  df_I = df_I.pivot(index=['idx','touchpointable_id'], columns='name', values='Weight').sort_index(level=1).reset_index().rename_axis(None, axis=1)
  df_I = df_I.fillna(0)
  df_I['Weight'] = df_I[col_list].sum(axis=1)
  df_I = pd.merge(df, df_I, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
  df_I = df_I.loc[:,~df_I.columns.duplicated()]
  
  
  
if len(interest) == 0:
  
  df_I = df
  df_I['Weight'] = 0
  

df_universities = df_universities.loc[df_universities['name'] == University]
city_name = df_universities.iloc[0]['city_name']
df_I['city score'] = np.where(df_I['city_name'] == city_name, 1,0)

df_I['degree score'] = np.where(df_I['name'] == Degree, 1,0)
df_E = df_I.loc[df_I['type'] == 'EducationRequirement']

id = df_E['id'].to_list()
df_E = df_I[~df_I.id.isin(id)]
df_E = pd.merge(df, df_E, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
df_E = df_E.loc[:,~df_E.columns.duplicated()]
df_D = df_I.loc[df_I['degree score'] == 1]
df_T = pd.merge(df, df_D, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
df_T = df_T.loc[:,~df_T.columns.duplicated()]
df_T = pd.concat([df_T,df_E])

df_subjects = df_subjects.loc[df_subjects['name'] == Subject]
df_subjects['subject score'] = 0.5
df_T = pd.merge(df_T,df_subjects, left_on='name',right_on='name_x',suffixes=('', '_x'),how = 'left')
df_T = df_T.loc[:,~df_T.columns.duplicated()]
df_S = df_T.loc[df_T['subject score'] == 0.5]
df_S = pd.merge(df, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
df_S = df_S.loc[:,~df_S.columns.duplicated()]
id = df_S['id'].to_list()
df_T = df_T[~df_T.id.isin(id)]
df_T = pd.concat([df_T,df_S])
df_T['year score'] = np.where(df_T['name'] == Year, 1,0)
df_Y = df_T.loc[df_T['year score'] == 1]
df_Y = pd.merge(df, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
df_Y = df_Y.loc[:,~df_Y.columns.duplicated()]
id_y = df_Y['id'].to_list()
df_T = df_T[~df_T.id.isin(id_y)]
df_A =  pd.concat([df_T,df_Y])
df_A = df_A[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','year score','value']].copy()
col_list = ['Weight','city score','degree score','subject score','year score']
df_A['matching score'] = df_A[col_list].sum(axis=1)

df_A = df_A.sort_values(by='matching score',ascending=False)
df_O = df_I.loc[df_I['name'] == 'Open to All Students']
df_O = pd.merge(df, df_O, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
df_O = df_O.loc[:,~df_O.columns.duplicated()]

df_O = pd.merge(df_O,df_subjects, left_on='name',right_on='name_x',suffixes=('', '_x'),how = 'left')
df_O = df_O.loc[:,~df_O.columns.duplicated()]
df_S = df_O.loc[df_O['subject score'] == 0.5]
df_S = pd.merge(df, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
df_S = df_S.loc[:,~df_S.columns.duplicated()]
id = df_S['id'].to_list()
df_O = df_O[~df_O.id.isin(id)]
df_O = pd.concat([df_O,df_S])
df_O = df_O[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','value']].copy()
col_list = ['Weight','city score','degree score','subject score']
df_O['matching score'] = df_O[col_list].sum(axis=1)
df_O = df_O.sort_values(by='matching score',ascending=False)
df_A = pd.concat([df_A,df_O])

df_A = df_A.groupby('id', as_index=False).first()
df_A = df_A.sort_values(by='matching score',ascending=False)
kind = df_A['kind'].unique()

if "Start my Career with a Spring Week":
  
  if "Spring Weeks" in kind:
    
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Spring Weeks")
    df_A = df_A.sort_values(by='matching score',ascending=True)
    print(df_S)
    df_S = df_S.sample(frac = 0.7)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)

  if 'Virtual Internship' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_V = group_2.get_group("Virtual Internship")
    df_V = df_V.sample(frac = 0.3)
  else:
    df_V = df_A.iloc[:1]
    df_V = df_V.fillna(0)
  if 'Career Fairs' in kind:
    group_3 = df_A.groupby(df_A.kind)
    df_C = group_3.get_group("Career Fairs")
    df_C = df_C.sample(frac = 0.3)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Insight Days' in kind:
    group_4 = df_A.groupby(df_A.kind)
    df_I = group_4.get_group("Insight Days")
    df_I = df_I.sample(frac = 0.5)
  else:
    df_I = df_A.iloc[:1]
    df_I = df_I.fillna(0)
  if 'Competitions' in kind:
    group_5 = df_A.groupby(df_A.kind)
    df_c = group_5.get_group("Competitions")
    df_c = df_c.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)

df_1 = pd.concat([df_S,df_V])
df_1 = pd.concat([df_1,df_C])
df_1 = pd.concat([df_1,df_I])
df_1 = pd.concat([df_1,df_c])



if 'Get a Summer Internship' in goals:
  if "Summer" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Summer")
    df_S = df_S.sample(frac = 1.0)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Networking' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_V = group_2.get_group("Networking")
    df_V = df_V.sample(frac = 0.3)
  else:
    df_V = df_A.iloc[:1]
    df_V = df_V.fillna(0)
  if 'Career Fairs' in kind:
    group_3 = df_A.groupby(df_A.kind)
    df_C = group_3.get_group("Career Fairs")
    df_C = df_C.sample(frac = 0.3)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Insight Days' in kind:
    group_4 = df_A.groupby(df_A.kind)
    df_I = group_4.get_group("Insight Days")
    df_I = df_I.sample(frac = 0.2)
  else:
    df_I = df_A.iloc[:1]
    df_I = df_I.fillna(0)
  if 'Workshop' in kind:
    group_5 = df_A.groupby(df_A.kind)
    df_c = group_5.get_group("Workshop")
    df_c = df_c.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)

df_2 = pd.concat([df_S,df_V])
df_2 = pd.concat([df_2,df_C])
df_2 = pd.concat([df_2,df_I])
df_2 = pd.concat([df_2,df_c])

if 'Expand my Network & Connect with Industry Leaders ' in goals:
  if "Conference" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Conference")
    df_S = df_S.sample(frac = 0.2)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Networking' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_V = group_2.get_group("Networking")
    df_V = df_V.sample(frac = 0.2)
  else:
    df_V = df_A.iloc[:1]
    df_V = df_V.fillna(0)
  if 'Career Fairs' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Career Fairs")
    df_C = df_C.sample(frac = 0.2)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Insight Days' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_I = group_2.get_group("Insight Days")
    df_I = df_I.sample(frac = 0.2)
  else:
    df_I = df_A.iloc[:1]
    df_I = df_I.fillna(0)
  if 'Workshop' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_c = group_2.get_group("Workshop")
    df_c = df_c.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)

df_3 = pd.concat([df_S,df_V])
df_3 = pd.concat([df_3,df_C])
df_3 = pd.concat([df_3,df_I])
df_3 = pd.concat([df_3,df_c])


if 'Find a Co-founder & Start a Business' in goals:
  if "Conference" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Conference")
    df_S = df_S.sample(frac = 0.3)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Networking' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_V = group_2.get_group("Networking")
    df_V = df_V.sample(frac = 0.4)
  else:
    df_V = df_A.iloc[:1]
    df_V = df_V.fillna(0)
  if 'Competitions' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Competitions")
    df_C = df_C.sample(frac = 0.2)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  

df_4 = pd.concat([df_S,df_V])
df_4 = pd.concat([df_4,df_C])

if 'Meet Like-minded Students & join Societies' in goals:
  if "Conference" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Conference")
    df_S = df_S.sample(frac = 0.2)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Networking' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_V = group_2.get_group("Networking")
    df_V = df_V.sample(frac = 0.3)
  else:
    df_V = df_A.iloc[:1]
    df_V = df_V.fillna(0)
  if 'Competitions' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Competitions")
    df_C = df_C.sample(frac = 0.3)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Workshop' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_c = group_2.get_group("Workshop")
    df_c = df_c.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)
df_5 = pd.concat([df_S,df_V])
df_5 = pd.concat([df_5,df_C])
df_5 = pd.concat([df_5,df_c])


if 'Win Awards & Competitions' in goals:
  if "Conference" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Conference")
    df_S = df_S.sample(frac = 0.2)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'CompetitionsN' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Competitions")
    df_C = df_C.sample(frac = 0.6)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Workshop' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_c = group_2.get_group("Workshop")
    df_c = df_c.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)

df_6 = pd.concat([df_S,df_C])
df_6 = pd.concat([df_6,df_c])

if 'Secure a Graduate Job' in goals:
  if "Graduate Job" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Graduate Job")
    df_S = df_S.sample(frac = 1.0)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Networking' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Networking")
    df_C = df_C.sample(frac = 0.3)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Career Fairs' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_c = group_2.get_group("Career Fairs")
    df_c = df_c.sample(frac = 0.7)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)

df_7 = pd.concat([df_S,df_C])
df_7 = pd.concat([df_7,df_c])

if 'Land a Placement Year' in goals:
  if "Placement Programme" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Placement Programme")
    df_S = df_S.sample(frac = 1.0)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Networking' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Networking")
    df_C = df_C.sample(frac = 0.2)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Career Fairs' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_c = group_2.get_group("Career Fairs")
    df_c = df_c.sample(frac = 0.3)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)
  
  if 'Insight Days' in kind:
    group_4 = df_A.groupby(df_A.kind)
    df_I = group_4.get_group("Insight Days")
    df_I = df_I.sample(frac = 0.3)
  else:
    df_I = df_A.iloc[:1]
    df_I = df_I.fillna(0)
  if 'Workshop' in kind:
    group_5 = df_A.groupby(df_A.kind)
    df_V = group_5.get_group("Workshop")
    df_V = df_V.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)
df_8 = pd.concat([df_S,df_C])
df_8 = pd.concat([df_8,df_c])
df_8 = pd.concat([df_8,df_I])
df_8 = pd.concat([df_8,df_V])

if 'Get an Internship alongside my studies' in goals:
  if "Virtual Internship" in kind:
    group_1 = df_A.groupby(df_A.kind)
    df_S = group_1.get_group("Virtual Internship")
    df_S = df_S.sample(frac = 0.3)
  else:
    df_S = df_A.iloc[:1]
    df_S = df_S.fillna(0)
  if 'Off-cycle' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_C = group_2.get_group("Off-cycle")
    df_C = df_C.sample(frac = 0.7)
  else:
    df_C = df_A.iloc[:1]
    df_C = df_C.fillna(0)
  if 'Career Fairs' in kind:
    group_2 = df_A.groupby(df_A.kind)
    df_c = group_2.get_group("Career Fairs")
    df_c = df_c.sample(frac = 0.2)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)
  
  if 'Networking' in kind:
    group_4 = df_A.groupby(df_A.kind)
    df_I = group_4.get_group("Networking")
    df_I = df_I.sample(frac = 0.5)
  else:
    df_I = df_A.iloc[:1]
    df_I = df_I.fillna(0)
  if 'Conference' in kind:
    group_5 = df_A.groupby(df_A.kind)
    df_V = group_5.get_group("Conference")
    df_V = df_V.sample(frac = 0.3)
  else:
    df_c = df_A.iloc[:1]
    df_c = df_c.fillna(0)
df_9 = pd.concat([df_S,df_C])
df_9 = pd.concat([df_9,df_c])
df_9 = pd.concat([df_9,df_I])
df_9 = pd.concat([df_9,df_V])







matches = pd.concat([df_1,df_2])
matches = pd.concat([matches,df_3])
matches = pd.concat([matches,df_4])
matches = pd.concat([matches,df_5])
matches = pd.concat([matches,df_6])
matches = pd.concat([matches,df_7])
matches = pd.concat([matches,df_8])
matches = pd.concat([matches,df_9])
matches = matches.sort_values(by='matching score',ascending=False)
group_10  = matches.groupby(matches.touchpointable_type)
Internship = group_10.get_group("Internship")
Internship =  pd.merge(df, Internship, left_on='id',right_on='id',suffixes=('', '_x'),how = 'inner')
Internship = Internship.loc[:,~Internship.columns.duplicated()]
Internship = Internship.iloc[:20]
group_11 = matches.groupby(matches.touchpointable_type)
Events = group_11.get_group("Events")
Events =  pd.merge(df, Events, left_on='id',right_on='id',suffixes=('', '_x'),how = 'inner')
Events = Events.loc[:,~Events.columns.duplicated()]
Events = Events.iloc[:20]
group_12 = matches.groupby(matches.touchpointable_type)
Jobs = group_12.get_group("Graduate Job")
Jobs =  pd.merge(df, Jobs, left_on='id',right_on='id',suffixes=('', '_x'),how = 'inner')
Jobs = Jobs.loc[:,~Jobs.columns.duplicated()]
Jobs = Jobs.iloc[:20]
st.write(Internship)
st.write(Events)
st.write(Jobs)
