from .new_sql_con import sql_con_django

from dash import Dash, html, dcc, dash_table,get_asset_url
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from datetime import date
import datetime
import pandas as pd
from datetime import date
from isoweek import Week
from django_plotly_dash import DjangoDash

class sql_zap_baltika():
    sql_oktell_zapros='''
    SELECT   [week] as 'Период',
                    [year],
                    --round(cast(answen_before_X_second *100 as float) / cnt ,2) [SL,%],
                    round(cast((answen_before_X_second) *100 as float) / (cnt-cnt_operator_missed_) ,2) [SL до х сек,%],
                    cnt [Всего],
                    cnt_connected [Обслужено],
                    (cnt - cnt_connected)  [Потеряно],
                    cnt_missed [Не дождался в очереди],
                    round(cast(cnt_connected*100 as float)/cnt ,2) [Обслужено, %],
                    round(cast((cnt - cnt_connected )*100 as float)/cnt ,2) [Потеряно, %],
                    round(cast(cnt_missed*100 as float)/cnt ,2) [Не дождался в очереди, %],
                    cnt_passed_in [Внутренний перевод],
                    cnt_passed_out [Внешний перевод],
                    round(cast(cnt_passed_in*100 as float)/CASE WHEN cnt_connected = 0 THEN 1 ELSE cnt_connected END,2) [Внутренний перевод, %],
                    round(cast(cnt_passed_out*100 as float)/CASE WHEN cnt_connected = 0 THEN 1 ELSE cnt_connected END ,2) [Внешний перевод, %],
                    cnt_operator_missed_ [Количество потерянных до x секунд],
                    answen_before_X_second [Отвечено до X сек]
        FROM (
        SELECT      [week],
                    [year],
                    COUNT(tt.IdCHAIN) cnt,
                    SUM(have_res) + SUM(CASE WHEN have_operator = 1 AND cr1 = 1 AND missed = 0 AND block_b = 0 AND have_res = 0 AND operator_missed = 0 AND not_between_work_time =0 THEN 1 ELSE 0 END) cnt_connected,
                    SUM(passed_in) cnt_passed_in,
                    SUM(passed_out) cnt_passed_out,
                    SUM(CASE WHEN have_res = 0 AND
                                have_operator = 0 AND
                                cr1 = 0 AND
                                missed = 0 AND
                                block_b = 0 AND
                                operator_missed = 0 AND
                                not_between_work_time=0
                            THEN 1 ELSE 0 END) + SUM(missed)  cnt_missed,
                    SUM(block_b) + SUM(CASE WHEN have_operator = 0 AND cr1 = 1 AND missed = 0 AND block_b = 0 AND (have_res = 0 AND operator_missed = 0) AND (not_between_work_time =0) THEN 1 ELSE 0 END) cnt_block_b,
                    SUM(not_between_work_time) cnt_not_between_work_time,
                    SUM(CASE WHEN have_res = 0 AND
                                have_operator = 1 AND
                                cr1 = 0 AND
                                missed = 0 AND
                                block_b = 0 AND
                                operator_missed = 1 AND
                                not_between_work_time=0
                            THEN 1 ELSE 0 END) cnt_operator_missed,
                    SUM(CASE WHEN p_LenQueue <= CASE WHEN {time_sec}=0 THEN p_LenQueue ELSE {time_sec} END AND
                                missed = 1
                            THEN 1 ELSE 0 END) cnt_operator_missed_,
                    SUM(CASE WHEN p_LenQueue <= CASE WHEN {time_sec}=0 THEN p_LenQueue ELSE {time_sec} END AND (have_res = 1 OR (have_operator = 1 AND cr1 = 1 AND have_res = 0)) THEN 1 ELSE 0 END) answen_before_X_second 
            FROM (
            SELECT IdCHAIN,
                DateStart,
                DATEPART (iso_week ,DateStart) as [week],
                DATEPART (year,DateStart) as [year],
                CASE WHEN SUM(CASE WHEN CallResult IN (5,18) AND IdOperator != 'ab000000-0000-0000-0000-000000000000' THEN 1
                                    ELSE 0 END) > 0 THEN 1 ELSE 0 END have_res,
                MIN(DateTimeStart) DateTimeStart,
                MAX(DateTimeStart) MAX_DateTimeStart,
                CASE WHEN SUM(CASE WHEN LEFT(BLineNum,2) NOT IN ('11','12','13','14') AND COALESCE(IdBUser,'ab000000-0000-0000-0000-000000000000') NOT IN ( 'ab000000-0000-0000-0000-000000000000','bf000000-0000-0000-0000-000000000000') AND  LenTime > 0 THEN 1 ELSE 0 END ) > 1 THEN 1 ELSE 0 END passed_in,
                CASE WHEN SUM(CASE WHEN LEFT(BLineNum,2) IN ('11','12','13','14') AND LenTime > 0 THEN 1 ELSE 0 END ) > 0 THEN 1 ELSE 0 END passed_out,
                CASE WHEN SUM(CASE WHEN COALESCE(IdOperator,'ab000000-0000-0000-0000-000000000000') != 'ab000000-0000-0000-0000-000000000000' THEN 1 ELSE 0 END ) > 0 THEN 1 ELSE 0 END have_operator,
                CASE WHEN SUM(CASE WHEN CallResult IN (13) THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END missed,
                --(21,22,25,26,27) изменен на 0
                CASE WHEN SUM(CASE WHEN CallResult IN (21,22,25,26,27) THEN 0 ELSE 0 END) > 0 THEN 1 ELSE 0 END block_b,
                CASE WHEN SUM(CASE WHEN CallResult IN (23) THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END not_between_work_time,
                CASE WHEN SUM(CASE WHEN CallResult IN (7) THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END operator_missed,
                CASE WHEN SUM(CASE WHEN CallResult = 1 THEN 1 ELSE 0 END)  > 0 THEN 1 ELSE 0 END cr1,
                MAX(LenQueue) p_LenQueue
            FROM  oktell_cc_temp..A_Cube_CC_EffortConnections c
            WHERE CONVERT (date,DateTimeStart) between CONVERT (date,'{date_start}') AND CONVERT (date,'{date_end}')
                AND  c.IdTask ='32B34725-4D73-432F-8F55-688103EAB1B9'
                AND IdCHAIN IS NOT NULL
                AND COALESCE(IsOutput,0) = 0
                GROUP BY IdCHAIN, DateStart
            ) as tt
        GROUP BY  [week],
                    [year]                    
        )     t
            Order by 1, 2
        '''


    sql_zapros_stat='''
    select
    CAST(CONVERT(datetime,sum (case IdOperator
        when 'AB000000-0000-0000-0000-000000000000' THEN isnull(LenTime,0)
        when null then isnull(LenTime,0)
        else 0 end
        ) /100000) AS time ) as 'Время ожидания'
    ,
    CAST(CONVERT(datetime,sum (case when IdOperator!=  'AB000000-0000-0000-0000-000000000000' and IdOperator is not null then isnull(LenTime,0)
        else 0 end
        )/100000) AS time ) as 'Время разговора'
    ,
    CallResult,
    IdTask,
    IdChain,
    IdEffort ,
    MIN(DateTimeStart) as 'DateTimeStart' ,
    w.Name as name,
    q.AbonentNumber
    ,r10 as [Комментарий]
    ,r11 as [Результат звонка]
    from oktell_cc_temp.dbo.A_Cube_CC_EffortConnections q
    left join oktell_settings.dbo.A_TaskManager_Tasks w on q.IdTask = w.Id
    left join oktell.dbo.s081_Qverty_In e on q.IdChain=e.IdComChain
    WHERE 1=1
    and CONVERT (date, DateTimeStart) between CONVERT (date,'{date_start}') AND CONVERT (date,'{date_end}')
    and w.id='32B34725-4D73-432F-8F55-688103EAB1B9'
    and IdChain is not NULL
    GROUP by CallResult ,IdTask,q.AbonentNumber  ,IdEffort,name,IdChain ,r10,r11   
    '''


    sql_song='''SELECT 
    convert(date,TimeStart)
    ,convert(time,TimeStart)
    ,'mix'
    ,case when ALineNum not like '%e%' then ALineNum else BLineNum end
    ,case when BLineNum like '%e%' then BLineNum else ALineNum end
    ,TimeStart 
    from oktell.dbo.A_Stat_Connections_1x1
    WHERE 
    id in (SELECT 
    IdConn
    from oktell_cc_temp.dbo.A_Cube_CC_EffortConnections q
    where q.IdEffort ='{IdEffort}'
    and (q.IdOperator is not NULL  and q.IdOperator!='AB000000-0000-0000-0000-000000000000')
    and IdConn is not NULL GROUP by IdConn )'''

    sql_obshi_log='''
        SELECT 
        [week] as 'Период',
        [year],
        sum([Оформлен заказ]) as 'Оформлен заказ',
        sum([Согласен ожидать/заказ переназначен]) as 'Согласен ожидать/заказ переназначен',
        sum([Абонент записал контактные данные организации для самостоятельного обращения]) as 'Абонент записал контактные данные организации для самостоятельного обращения',
        sum([Просьба перезвонить]) as 'Просьба перезвонить',
        sum([Указаны неверные контакты организации]) as 'Указаны неверные контакты организации',
        sum([Отказ от заказа]) as 'Отказ от заказа',
        sum([Недозвон]) as 'Недозвон',
        sum([Занято]) as 'Занято',
        sum([Не отвечает]) as 'Не отвечает',
        sum([Сработал автоответчик / факс /Сбой связи]) as 'Сработал автоответчик / факс /Сбой связи',
        sum([Отказ от оформления]) as 'Отказ от оформления',
        sum([прочии]) as 'прочии'
        from (sELECT 
                        datetime_call,
                        DATEPART (iso_week ,datetime_call) as [week],
                        DATEPART (year,datetime_call) as [year],
                        case when com2 ='Отказ от оформления' then 1 else 0 end [Отказ от оформления],
                        case when com2 ='Оформлен заказ' then 1 else 0 end 'Оформлен заказ',
                        case when com2 ='Указаны неверные контакты организации' then 1 else 0 end 'Указаны неверные контакты организации',
                        case when com2 ='Занято' then 1 else 0 end 'Занято',
                        case when com2 ='Абонент записал контактные данные организации для самостоятельного обращения' then 1 else 0 end 'Абонент записал контактные данные организации для самостоятельного обращения',
                        case when com2 ='Просьба перезвонить' then 1 else 0 end 'Просьба перезвонить',
                        case when com2 ='Отказ от заказа' then 1 else 0 end 'Отказ от заказа',
                        case when com2 ='Недозвон' then 1 else 0 end 'Недозвон',
                        case when com2 ='Согласен ожидать/заказ переназначен' then 1 else 0 end 'Согласен ожидать/заказ переназначен',
                        case when com2 ='Сработал автоответчик / факс /Сбой связи' then 1 else 0 end 'Сработал автоответчик / факс /Сбой связи',
                        case when com2 ='Не отвечает' then 1 else 0 end 'Не отвечает',
                        case when com2 not in  ('Отказ от оформления','Оформлен заказ','Указаны неверные контакты организации','Занято','Абонент записал контактные данные организации для самостоятельного обращения','Просьба перезвонить','Отказ от заказа','Недозвон','-1','Согласен ожидать/заказ переназначен','Сработал автоответчик / факс /Сбой связи','Не отвечает') then 1 else 0 end 'прочии'
                    FROM  oktell.dbo.s081_AbonentList_log
        WHERE task_label =407
        and com2 is not null
        and convert(date,datetime_call) BETWEEN convert(date,'{date_start}') and convert(date,'{end_date}')) qwe
        GROUP by [week],[year]'''

    sql_log_list='''
        select 
        id ,operator_id ,task_id,convert (datetime,datetime_call,20) as datetime_call,phone,operator_name,com1,com2,r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14,r15,r16,r17
        FROM  oktell.dbo.s081_AbonentList_log
        WHERE task_label =407
        and com2 is not null
        and convert(date,datetime_call) BETWEEN convert(date,'{date_start}') and convert(date,'{end_date}')
        '''
    sql_son_oper='''
        SELECT 
        IdOperator ,MIN(DateTimeStart) as DateTimeStart ,max(DateTimeStop) as DateTimeStop
        from oktell_cc_temp.dbo.A_Cube_CC_OperatorStates
        WHERE IdOperator ='{IdOperator}'
        and IdTask ='{IdTask}'
        and convert (datetime,DateTimeStart,20) <=convert (datetime,'{date_start}',20)
        and convert (datetime,DateTimeStop ,20) >=convert (datetime,'{date_start}',20)
        GROUP by IdOperator'''
    sql_song__='''SELECT 
        convert(date,TimeStart)
        ,convert(time,TimeStart)
        ,'mix'
        ,case when ALineNum not like '%e%' then ALineNum else BLineNum end
        ,case when BLineNum like '%e%' then BLineNum else ALineNum end
        ,TimeStart
        ,SUBSTRING ( (case when len(AOutNumber)<11 then  BOutNumber else AOutNumber end) , 4, len((case when len(AOutNumber)<11 then  BOutNumber else AOutNumber end)) )  
    from oktell.dbo.A_Stat_Connections_1x1
    WHERE convert(datetime,TimeStart,20) >=convert(datetime,'{date_start}',20)
    and convert(datetime,TimeStop,20) <=convert(datetime,'{end_date}',20)
    and (AUserId ='{IdOperator}' or BUserId ='{IdOperator}')
    '''



external_stylesheets=[dbc.themes.BOOTSTRAP]
app = DjangoDash('lplp'
                    , external_stylesheets=external_stylesheets
                    )

app.layout = html.Div([
    html.Div([
        dcc.Tabs([
            dcc.Tab(label='Входящий' ,children=[ html.Div([ 
                html.Div([ 
                    html.Div([
                                html.Div([
                                html.Span(['С  :'],className="input-group-text")
                                ],id='peremen_0_1',className="input-group-prepend"),
                                dcc.Input(id="date_start", type="date",className="form-control",
                                        value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),
                                        ),
                                html.Div([            
                                html.Span(['ПО :'],className="input-group-text")
                                ],id='peremen_0_2',className="input-group-prepend"),        
                                dcc.Input(id="date_end", type="date",className="form-control",
                                        value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
                                        ),
                                html.Div([
                                    html.Span(['X сек. ожидания до ответа (0 - не учитывать):'],className="input-group-text")
                                ],id='peremen_1_1',className="input-group-prepend"),
                                dcc.Input(id="time_sec", type="number", value='0',className="form-control"),        
                    ],id='peremen',className="input-group"),
            
                    html.Div([html.Button('загрузить', id='down_sql',className="btn btn-outline-secondary", n_clicks=None
                                ,style={'width': '100%'}
                                )
                    ],id='drow',className="input-group")
                    

                ]), 
                dcc.Tabs([
                    dcc.Tab(label='Сводная по неделям' ,children=[
                        html.Div([
                        ],id='table')   
                        ],id='vhod-1_div'),
                    dcc.Tab(label='Расшифровка по звонкам' ,children=[ html.Div([ 
                        html.Div([
                        ],id='table2'),
                        html.Div([
                        ],id='table1')                                                        

                    ],id='vhod-2_div')]),
                ]),


            ],id='vhod-div')]),
            dcc.Tab(label='Исходящие',children=[ html.Div([
html.Div([ 
                    html.Div([
                                html.Div([
                                html.Span(['С  :'],className="input-group-text")
                                ],id='peremen_0_1_i',className="input-group-prepend"),
                                dcc.Input(id="date_start_i", type="date",className="form-control",
                                        value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),
                                        ),
                                html.Div([            
                                html.Span(['ПО :'],className="input-group-text")
                                ],id='peremen_0_2_i',className="input-group-prepend"),        
                                dcc.Input(id="date_end_i", type="date",className="form-control",
                                        value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
                                        ),
    
                    ],id='peremen_i',className="input-group"),
            
                    html.Div([html.Button('загрузить', id='down_sql_i',className="btn btn-outline-secondary", n_clicks=None
                                ,style={'width': '100%'}
                                )
                    ],id='drow_i',className="input-group")
                    

                ]), 
                dcc.Tabs([
                    dcc.Tab(label='Сводная по неделям' ,children=[
                        html.Div([
                        ],id='table_i')   
                        ],id='1_div_i'),
                    dcc.Tab(label='Расшифровка по звонкам' ,children=[ html.Div([ 
                        html.Div([
                        ],id='table2_i'),
                        html.Div([
                        ],id='table1_i')                                                        

                    ],id='2_div_i')]),
                ]),



                    ],id='ishod-div')])
        ])

    ]),

])


@app.callback(
    Output('table2', component_property='children'),
    Input('table_1_2', 'selected_row_ids')
)
def update_graphs(selected_row_ids):
    song_path=[]
    row=[]
    div=[]
    try:
        if len(selected_row_ids) != 0:
            data=str(selected_row_ids).replace("['",'').replace("']",'').split('$$')
            for i in data:
                row.append(i.split('='))
            row_key = {j[0] : j[1] for j in row} 
            IdTask=row_key.get('IdTask') 
            IdEffort=row_key.get('IdEffort') 
            sql_zap_song=sql_zap_baltika.sql_song.format(IdEffort=IdEffort) 
            df_table_song=sql_con_django(sql=sql_zap_song).applymap(str).values.tolist()
            for i in df_table_song:
                text_song=i[5].split('.')
                song=['\\'+str(i[0]).replace('-','')+'\\'+str(i[1][:5]).replace(':','')+'\\'+str(i[2])+'_'+str(i[3])+'_'+str(i[4])+'__'+str(text_song[0]).replace('-','_').replace(':','_').replace(' ','__')+'_'+str(text_song[1][:3])+'.mp3',i[5]]
                
                song_path.append(song)
            import os

                    
                    
            #print(song_path)
            columns_song=['Дата','Запись']
            for i in song_path:
                from pathlib import Path
                import os
                src=Path('\\\\qqq\\RecordedFiles${}'.format(i[0]))
                dest =Path(str(src).replace('\\\\qqq\\RecordedFiles$','assets'))
                
                dest_path=str(dest).split('\\mix')
                assets_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static')
                os.makedirs(assets_path+'\\{}'.format(dest_path[0]), exist_ok=True)
                media=Path(str(assets_path+'\\{}'.format(dest)))
                media.write_bytes(src.read_bytes()) 
                dest_list=(str(dest)).split('\\')
                dest_src=''
                for d in dest_list:
                    dest_src=dest_src+'/'+d
                text_date=str(i[1]).split('.')
                div.append(
                    html.Div([
                        html.Div([
                        html.Span([text_date[0]],className='input-group-text')
                        ],className="input-group-text")
                        ,html.Audio(src="/static{}".format(dest_src) , controls=True,className="form-control") 
                    ],className="input-group")
                        )
                
    
        return html.Div(div,className="input-group")
    except:
        return    

@app.callback(
    Output(component_id='down_sql', component_property='n_clicks'),
    Output(component_id='table', component_property='children'),
    Output(component_id='table1', component_property='children'),
    Output('date_start', 'value'),
    Output('date_end', 'value'),        
    Input("date_start", "value"),
    Input("date_end", "value"),
    Input(component_id='down_sql', component_property='n_clicks'),
    Input("time_sec", "value")
)
def update_output(date_start,date_end,n_clicks,time_sec):
    if n_clicks is None:
        n_clicks=0
        return n_clicks,'','',date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))), date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
    elif n_clicks==0:
        raise PreventUpdate    
    else:
        n_clicks=0 
    

        
        sql_table=sql_zap_baltika.sql_oktell_zapros.format(date_start=str(date_start),date_end=str(date_end),time_sec=time_sec) 
        
        df_table=sql_con_django(sql=sql_table).applymap(str)
        #print(df_table)
        df_data=df_table.values.tolist()
        for i in df_data:
            week=i[0]
            year=i[1]
            interval=Week(int(year), int(week)).monday().strftime('%Y-%m-%d')+' '+Week(int(year),int(week)).sunday().strftime('%Y-%m-%d')
            df_table.loc[(df_table['Период'] == week ) & (df_table['year'] == year),'Период']=interval
        sql_table1=sql_zap_baltika.sql_zapros_stat.format(date_start=str(date_start),date_end=str(date_end))   
        
        df_table1=sql_con_django(sql=sql_table1).applymap(str)
        df_table1['Время ожидания']=df_table1['Время ожидания']. str.split('.', expand= True )[0]
        df_table1['Время разговора']=df_table1['Время разговора']. str.split('.', expand= True )[0]
        
        
        df_table1.loc[df_table1['CallResult'] =='13', 'CallResult'] = 'Абонент прервал ожидание в очереди'
        df_table1.loc[df_table1['CallResult'] =='29', 'CallResult'] = 'успех'
        df_table1.loc[df_table1['CallResult'] =='1', 'CallResult'] = 'Система сработала некорректно'
        df_table1.loc[df_table1['CallResult'] =='8', 'CallResult'] = 'успех'
        df_table1.loc[df_table1['CallResult'] =='5', 'CallResult'] = 'успех'
        df_table1.loc[df_table1['CallResult'] =='26', 'CallResult'] = 'Операторы отсутствует'
        df_table1.loc[df_table1['CallResult'] =='28', 'CallResult'] = 'Сбой'
        df_table1['Результат коммутации']=df_table1['CallResult']
        df_table1['id']='IdTask='+df_table1['IdTask']+'$$IdEffort='+df_table1['IdEffort']

        return n_clicks,html.Div([dash_table.DataTable(
        style_table={'overflowY': 'auto',},
        id='table_1_1',
        columns=[{"name": i, "id": i} for i in df_table.columns
        if i not in ('year')
            ],
        page_size=20,
        #editable=True,
        sort_mode="multi",
        #row_selectable="single",
        export_format='xlsx',
        sort_action='native',
        data=df_table.to_dict('records'))]),html.Div([dash_table.DataTable(
        #style_table={'overflowY': 'auto',},
        style_data={
        'whiteSpace': 'normal',
        'height': 'auto',
        },
        id='table_1_2',
        columns=[{"name": i, "id": i} for i in df_table1.columns
            if i not in ('id','year','IdEffort','IdTask','CallResult','name','year')
            ],
        page_size=10,
        #editable=True,
        sort_mode="multi",
        row_selectable="single",
        selected_columns=[],
        selected_rows=[],
        export_format='xlsx',
        sort_action='native',
        data=df_table1.to_dict('records'))]),date_start,date_end

@app.callback(
    Output(component_id='down_sql_i', component_property='n_clicks'),
    Output(component_id='table_i', component_property='children'),
    Output(component_id='table1_i', component_property='children'),  
    Output('date_start_i', 'value'),
    Output('date_end_i', 'value'),        
    Input("date_start_i", "value"),
    Input("date_end_i", "value"),
    Input(component_id='down_sql_i', component_property='n_clicks')
)
def update_log(date_start,date_end,n_clicks):
    if n_clicks is None:
        n_clicks=0
        return n_clicks,'','',date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))), date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
    elif n_clicks==0:
        raise PreventUpdate    
    else:
        n_clicks=0 
    

        
        sql_table=sql_zap_baltika.sql_obshi_log.format(date_start=str(date_start),end_date=str(date_end)) 
        
        df_table=sql_con_django(sql=sql_table).applymap(str)
        #print(df_table)
        df_data=df_table.values.tolist()
        for i in df_data:
            week=i[0]
            year=i[1]
            interval=Week(int(year), int(week)).monday().strftime('%Y-%m-%d')+' '+Week(int(year),int(week)).sunday().strftime('%Y-%m-%d')
            df_table.loc[(df_table['Период'] == week ) & (df_table['year'] == year),'Период']=interval
        sql_table1=sql_zap_baltika.sql_log_list.format(date_start=str(date_start),end_date=str(date_end))   
        
        df_table1=sql_con_django(sql=sql_table1).applymap(str)
        df_table1['id']='IdOperator='+df_table1['operator_id']+'$$datetime_call='+df_table1['datetime_call']+'$$IdTask='+df_table1['task_id']
        


        return n_clicks,html.Div([dash_table.DataTable(
        style_table={'overflowY': 'auto',},
        id='table_1_1_i',
        columns=[{"name": i, "id": i} for i in df_table.columns
        if i not in ('year')
            ],
        page_size=20,
        #editable=True,
        sort_mode="multi",
        #row_selectable="single",
        export_format='xlsx',
        sort_action='native',
        data=df_table.to_dict('records'))]),html.Div([dash_table.DataTable(
        #style_table={'overflowY': 'auto',},
#        style_data={
#        'whiteSpace': 'normal',
#        'height': 'auto',
#        },
        id='table_1_2_i',
        columns=[{"name": i, "id": i} for i in df_table1.columns
            if i not in ('id','operator_id','task_id','IdTask')
            ],
        page_size=10,
        #editable=True,
        sort_mode="multi",
        row_selectable="single",
        selected_columns=[],
        selected_rows=[],
        export_format='xlsx',
        sort_action='native',
        data=df_table1.to_dict('records'))]),date_start,date_end    
    

@app.callback(
    Output('table2_i', component_property='children'),
    Input('table_1_2_i', 'selected_row_ids')
)
def log_song(selected_row_ids):
    row=[]
    song_path=[]
    div=[]
    print(selected_row_ids)
    if selected_row_ids:
        data=str(selected_row_ids).replace("['",'').replace("']",'').split('$$')
        for i in data:
            row.append(i.split('='))
        row_key = {j[0] : j[1] for j in row} 
        IdTask=row_key.get('IdTask') 
        IdOperator=row_key.get('IdOperator') 
        date_start=row_key.get('datetime_call')
        if date_start.find( '.') >=0:
            date_start=date_start[:-3]

        sql_zap_song=sql_zap_baltika.sql_son_oper.format(IdOperator=IdOperator,IdTask=IdTask,date_start=date_start) 
        df_table_song=sql_con_django(sql=sql_zap_song).applymap(str).values.tolist()
        if df_table_song[0][1].find( '.') >=0:
            date_start=df_table_song[0][1][:-3]
        if df_table_song[0][2].find( '.') >=0:
            end_date=df_table_song[0][2][:-3]    
        sql_zap_song=sql_zap_baltika.sql_song__.format(IdOperator=df_table_song[0][0],date_start=date_start,end_date=end_date) 
        df_table_song_=sql_con_django(sql=sql_zap_song).applymap(str).values.tolist()
        for i in df_table_song_:
            text_song=i[5].split('.')
            song=['\\'+str(i[0]).replace('-','')+'\\'+str(i[1][:5]).replace(':','')+'\\'+str(i[2])+'_'+str(i[3])+'_'+str(i[4])+'__'+str(text_song[0]).replace('-','_').replace(':','_').replace(' ','__')+'_'+str(text_song[1][:3])+'.mp3',i[5],i[6]]
            
        #    song_path.append(song)
        #for i in song_path:
            from pathlib import Path
            import os
            src=Path('\\\\OKTELL-2023\\RecordedFiles${}'.format(song[0]))
            dest =Path(str(src).replace('\\\\OKTELL-2023\\RecordedFiles$','assets'))
            
            dest_path=str(dest).split('\\mix')
            assets_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static')
            os.makedirs(assets_path+'\\{}'.format(dest_path[0]), exist_ok=True)
            media=Path(str(assets_path+'\\{}'.format(dest)))
            media.write_bytes(src.read_bytes()) 
            dest_list=(str(dest)).split('\\')
            dest_src=''
            for d in dest_list:
                dest_src=dest_src+'/'+d
            print(dest_src)
            text_date=str(song[1]).split('.')
            div.append(
                html.Div([
                    html.Div([
                    html.Span([text_date[0]+' '+song[2]],className='input-group-text')
                    ],className="input-group-text")
                    ,html.Audio(src="/static{}".format(dest_src) , controls=True,className="form-control") 
                ],className="input-group")
                    )
                        
            
        return html.Div(div,className="input-group")  