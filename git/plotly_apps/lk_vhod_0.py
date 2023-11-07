import pandas as pd
import datetime 
from django_plotly_dash import DjangoDash
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import dash
import dash_bootstrap_components as dbc
try:
    from dash import dcc
    from dash import html   
except:
    import dash_core_components as dcc
    import dash_html_components as html
import plotly.express as px
from dash import dash_table
from datetime import date
from .lk_vhod_0_temp import task_sql

def sql_con_django(sql):
    from django.db import connection
    import pandas as pd
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = list(cursor.fetchall())
        head = ([column[0] for column in cursor.description]) 
        data = pd.DataFrame(data,columns=head) 
    return data  



external_stylesheets=[dbc.themes.BOOTSTRAP]
app = DjangoDash(
                    'lk_vhod_0'
                    , external_stylesheets=external_stylesheets
                    )


task=sql_con_django(sql=task_sql.task_sql).applymap(str).values.tolist()
task_list=[{"label": i[1], "value": i[0].upper()} for i in task]
app.layout = html.Div([
            dcc.Tabs([
                    dcc.Tab(label='Отчет Детализация входящих вызовов' ,children=[
                       html.Div([
                            html.Div([
                                            html.Div(id='song_1'),
                                            dcc.Dropdown(options=task_list,multi=True, id='dropdown_selection_1'),
                                            html.Div([
                                                html.Div([
                                                html.Span(['ДАТА:'],className="input-group-text")
                                                ],id='peremen_1_1',className="input-group-prepend"),
                                                dcc.Input(id="date_start_1", type="date",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),
                                                ),
                                                dcc.Input(id="end_date_1", type="date",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
                                                ),
                                                html.Div([html.Button('загрузить', id='down_sql_i_1',className="btn btn-outline-secondary", style={'width': '100%'})
                                                ],id='drow_i_1',className="input-group")
                                            ],id='peremen_1_2',className="input-group")
                                        ])
                            ],id='menu_select_1'),
                            html.Div(id='table_select_1')
                    ],id='1_div_i'),
                    dcc.Tab(label='Онлайн статистика' ,children=[
                        html.Div([
                            html.Div([
                                            dcc.Dropdown(options=task_list,multi=True, id='dropdown_selection_2'),
                                            html.Div([
                                                html.Div([
                                                html.Span(['ДАТА:'],className="input-group-text")
                                                ],id='peremen_2_1',className="input-group-prepend"),
                                                dcc.Input(id="date_start_2", type="date",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),
                                                ),
                                                dcc.Input(id="end_date_2", type="date",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
                                                ),
                                                html.Div([html.Button('загрузить', id='down_sql_i_2',className="btn btn-outline-secondary", style={'width': '100%'})
                                                ],id='drow_i_2',className="input-group")
                                            ],id='peremen_2_2',className="input-group")
                                        ])
                            ],id='menu_select_2'),
                            html.Div(id='table_select_2')
                    ],id='2_div_i'),
                    dcc.Tab(label='Ежедневная отчетность' ,children=[
                        html.Div([
                            html.Div([      
                                             html.Div(id='song_3'),   
                                            dcc.Dropdown(options=task_list,multi=True, id='dropdown_selection_3'),
                                            html.Div([
                                                html.Div([
                                                html.Span(['ДАТА:'],className="input-group-text")
                                                ],id='peremen_3_1',className="input-group-prepend"),
                                                dcc.Input(id="date_start_3", type="date",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),
                                                ),
                                                dcc.Input(id="end_date_3", type="date",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
                                                ),
                                                html.Div([html.Button('загрузить', id='down_sql_i_3',className="btn btn-outline-secondary",style={'width': '100%'})
                                                ],id='drow_i_3',className="input-group")
                                            ],id='peremen_3_2',className="input-group")
                                        ])
                            ],id='menu_select_3'),
                            html.Div(id='table_select_3')
                    ],id='3_div_i'),
                    dcc.Tab(label='Ежемесячная отчетность' ,children=[
                        html.Div([
                                            dcc.Dropdown(options=task_list,multi=True, id='dropdown_selection_4'),
                                            html.Div([
                                                html.Div([
                                                html.Span(['ДАТА:'],className="input-group-text")
                                                ],id='peremen_4_1',className="input-group-prepend"),
                                                dcc.Input(id="date_start_4", type="month",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),
                                                ),
                                                dcc.Input(id="end_date_4", type="month",className="form-control",
                                                value=date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d")))
                                                ),
                                                html.Div([html.Button('загрузить', id='down_sql_i_4',className="btn btn-outline-secondary",style={'width': '100%'})
                                                ],id='drow_i_4',className="input-group")
                                            ],id='peremen_4_2',className="input-group"),
                            html.Div(id='table_select_4')  
                                          
                            ],id='menu_select_4')
                    ],id='4_div_i')
                    ])  
    ])

@app.callback(
    Output('song_3', component_property='children'),
    Input('table_3', 'selected_row_ids')
)
def update_graphs(selected_row_ids):
    #print(selected_row_ids)
    song_path=[]
    row=[]
    row1=[]
    div=[]
    try:
        if len(selected_row_ids) != 0:
            IdTask,IdEffort=str(selected_row_ids).replace('"','').replace("[",'').replace("]",'').split('&&')
            row.append(IdTask.split('='))
            row1.append(IdEffort.split('=')) 
            row_key = {j[0] : j[1] for j in row} 
            IdTask=row_key.get('IdTask') 
            row1_key = {j[0] : j[1] for j in row1} 
            IdEffort=row1_key.get('IdEffort') 
            sql_zap_song=task_sql.sql_song.format(IdEffort=IdEffort,Task_Id=IdTask) 
            df_table_song=sql_con_django(sql=sql_zap_song).applymap(str).values.tolist()
            for i in df_table_song:
                text_song=i[5].split('.')
                song=['\\'+str(i[0]).replace('-','')+'\\'+str(i[1][:5]).replace(':','')+'\\'+str(i[2])+'_'+str(i[3])+'_'+str(i[4])+'__'+str(text_song[0]).replace('-','_').replace(':','_').replace(' ','__')+'_'+str(text_song[1][:3])+'.mp3',i[5]]
                
                song_path.append(song)
            import os
            columns_song=['Дата','Запись']
            dest_list=[]
            for i in song_path:
                from pathlib import Path
                import os
                src=Path('\\\\Oqqq\\RecordedFiles${}'.format(i[0]))
                dest =Path(str(src).replace('\\\\Oqqqq\\RecordedFiles$','assets'))
                dest_list.append(dest)
                dest_path=str(dest).split('\\mix')
                assets_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static')
                os.makedirs(assets_path+'\\{}'.format(dest_path[0]), exist_ok=True)
                media=Path(str(assets_path+'\\{}'.format(dest)))
                media.write_bytes(src.read_bytes()) 
            schet_list=0    
            for song in  song_path:
                text_date=str(song[1]).split('.')
                div.append(
                    html.Div([
                        html.Div([
                        html.Span([text_date[0]],className='input-group-text')
                        ],className="input-group-text")
                        ,html.Audio(src="/static/{}".format(dest_list[schet_list]) , controls=True,className="form-control") 
                    ],className="input-group")
                        )
                schet_list+=1
    
        return html.Div(div,className="input-group")
    except:
        return 

@app.callback(
    Output(component_id='down_sql_i_1', component_property='n_clicks'),
    Output('date_start_1', 'value'),
    Output('end_date_1', 'value'), 
    Output(component_id='table_select_1', component_property='children'), 
    Input("dropdown_selection_1", "value"),
    Input("date_start_1", "value"),
    Input("end_date_1", "value"),
    Input(component_id="down_sql_i_1", component_property="n_clicks")
)
def update_output_1(dropdown_selection_1,date_start_1,end_date_1,n_clicks_1):
    if n_clicks_1!=0:
        if dropdown_selection_1!=None:
            dropdown_selection_1=str(dropdown_selection_1).replace('[','(').replace(']',')')
            sql=task_sql.sql_vhod_otchet_1_2.format(Task_Id=dropdown_selection_1,DateTimeStart=date_start_1,DateTimeStop=end_date_1)
            #print(sql)
            df_sql_vhod_otchet_1=sql_con_django(sql=sql).applymap(str)
            sql=task_sql.sql_vhod_otchet_2_2.format(Task_Id=dropdown_selection_1,DateTimeStart=date_start_1,DateTimeStop=end_date_1)
            #print(sql)
            df_sql_vhod_otchet_2=sql_con_django(sql=sql).applymap(str)
            sql=task_sql.sql_vhod_otchet_3_2.format(Task_Id=dropdown_selection_1,DateTimeStart=date_start_1,DateTimeStop=end_date_1)
            #print(sql)
            df_sql_vhod_otchet_3=sql_con_django(sql=sql).applymap(str)
            sql=task_sql.sql_vhod_otchet_4_2.format(Task_Id=dropdown_selection_1)
            df_sql_vhod_otchet_4=sql_con_django(sql=sql).applymap(str).values.tolist()
            df_table_sql_vhod_otchet_1=pd.merge(df_sql_vhod_otchet_3,df_sql_vhod_otchet_1, how='left', on=['IdEffort'])

            df_table_sql_vhod_otchet=pd.merge(df_table_sql_vhod_otchet_1,df_sql_vhod_otchet_2,how='left',on=['IdEffort'])
            for i in df_sql_vhod_otchet_4:
                #print(i)
                df_table_sql_vhod_otchet['Номер очереди/наименование очереди']=df_table_sql_vhod_otchet['Номер очереди/наименование очереди'].replace([i[1]],i[0])
            df_table_sql_vhod_otchet12=df_table_sql_vhod_otchet[['IdEffort','Дата звонка/Время звонка', 
       'Номер очереди/наименование очереди', 'Номер оператора/ФИО оператора',
       'АОН клиента', 'Время ожидания','Разговор по задаче','Поствызывная обработка', 'Номер перевода (если был)']]
            df_table_sql_vhod_otchet12['id']='IdTask='+dropdown_selection_1+'&&IdEffort='+df_table_sql_vhod_otchet12['IdEffort']
            df_table_sql_vhod_otchet12=df_table_sql_vhod_otchet12.sort_values(by='Дата звонка/Время звонка', ascending=False)
            
            return 0,date_start_1,end_date_1,html.Div([dash_table.DataTable(
            style_table={'overflowY': 'auto',},
            id='table_1',
            columns=[{"name": i, "id": i} for i in df_table_sql_vhod_otchet12.columns
                     if i not in ('id','IdEffort')],
            page_size=20,
            #sort_mode="multi",
            #row_selectable="single",
            export_format='xlsx',
            export_headers='display',
            sort_action='native',
            data=df_table_sql_vhod_otchet12.to_dict('records'))])
    elif n_clicks_1 is None:
        return 0,date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),''   
    raise PreventUpdate  


@app.callback(
    Output(component_id='down_sql_i_2', component_property='n_clicks'),
    Output('date_start_2', 'value'),
    Output('end_date_2', 'value'), 
    Output(component_id='table_select_2', component_property='children'), 
    Input("dropdown_selection_2", "value"),
    Input("date_start_2", "value"),
    Input("end_date_2", "value"),
    Input(component_id="down_sql_i_2", component_property="n_clicks")
)
def update_output_2(dropdown_selection_2,date_start_2,end_date_2,n_clicks_2):
    if n_clicks_2!=0:
        if dropdown_selection_2!=None: 
            dropdown_selection_2=str(dropdown_selection_2).replace('[','(').replace(']',')')
            sql=task_sql.table_sql_otchet.format(IdTask=dropdown_selection_2,DateTimeStart=date_start_2,DateTimeStop=end_date_2)
            #print(sql)
            df_table_sql_otchet=sql_con_django(sql=sql).applymap(str)
            return 0,date_start_2,end_date_2,html.Div([dash_table.DataTable(
            style_table={'overflowY': 'auto',},
            id='table_2',
            columns=[{"name": i, "id": i} for i in df_table_sql_otchet.columns],
            page_size=20,
            export_format='xlsx',
            export_headers='display',
            sort_action='native',
            data=df_table_sql_otchet.to_dict('records'))])
    elif n_clicks_2 is None:
        return 0,date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),''   
    raise PreventUpdate  

@app.callback(
    Output(component_id='down_sql_i_3', component_property='n_clicks'),
    Output('date_start_3', 'value'),
    Output('end_date_3', 'value'), 
    Output(component_id='table_select_3', component_property='children'), 
    Input("dropdown_selection_3", "value"),
    Input("date_start_3", "value"),
    Input("end_date_3", "value"),
    Input(component_id="down_sql_i_3", component_property="n_clicks")
)
def update_output_3(dropdown_selection_3,date_start_3,end_date_3,n_clicks_3):
    if n_clicks_3!=0:
        
        if dropdown_selection_3!=None:
            dropdown_selection_3=str(dropdown_selection_3).replace('[','(').replace(']',')')
            sql=task_sql.sql_vhod_otchet_1.format(Task_Id=dropdown_selection_3,DateTimeStart=date_start_3,DateTimeStop=end_date_3)
            
            df_sql_vhod_otchet_1=sql_con_django(sql=sql).applymap(str)
            sql=task_sql.sql_vhod_otchet_2.format(Task_Id=dropdown_selection_3,DateTimeStart=date_start_3,DateTimeStop=end_date_3)
            
            df_sql_vhod_otchet_2=sql_con_django(sql=sql).applymap(str)
            sql=task_sql.sql_vhod_otchet_3.format(Task_Id=dropdown_selection_3,DateTimeStart=date_start_3,DateTimeStop=end_date_3)
            
            df_sql_vhod_otchet_3=sql_con_django(sql=sql).applymap(str)
            df_table_sql_vhod_otchet_1=df_sql_vhod_otchet_1.merge(df_sql_vhod_otchet_3, on=['IdComChain'])
            df_table_sql_vhod_otchet=df_table_sql_vhod_otchet_1.merge(df_sql_vhod_otchet_2, on=['IdEffort'])

            df_table_sql_vhod_otchet12=df_table_sql_vhod_otchet[['IdEffort'
        ,'Дата звонка/Время звонка', 'Звонок поступил на номер:','ФИО Абонента: ','Наименование организации абонента:','Контактный телефон абонента: ',
       'Суть обращения:', 'Результат звонка: ','Статус маршрутизации','Номер перевода (если был)','Статус перевода на внутренний номер',
       'Разговор по задаче в мин.','Номер оператора/ФИО оператора']]
            df_table_sql_vhod_otchet12['id']='IdTask='+dropdown_selection_3+'&&IdEffort='+df_table_sql_vhod_otchet12['IdEffort']
            df_table_sql_vhod_otchet12=df_table_sql_vhod_otchet12.sort_values(by='Дата звонка/Время звонка', ascending=False)
            return 0,date_start_3,end_date_3,html.Div([dash_table.DataTable(
            style_table={'overflowY': 'auto',},
            id='table_3',
            columns=[{"name": i, "id": i} for i in df_table_sql_vhod_otchet12.columns
                     if i not in ('id','IdEffort')],
            page_size=20,
            sort_mode="multi",
            row_selectable="single",
            export_format='xlsx',
            export_headers='display',
            sort_action='native',
            data=df_table_sql_vhod_otchet12.to_dict('records'))])
    elif n_clicks_3 is None:
        return 0,date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),''   
    raise PreventUpdate  


@app.callback(
    Output(component_id='down_sql_i_4', component_property='n_clicks'),
    Output('date_start_4', 'value'),
    Output('end_date_4', 'value'), 
    Output(component_id='table_select_4', component_property='children'), 
    Input("dropdown_selection_4", "value"),
    Input("date_start_4", "value"),
    Input("end_date_4", "value"),
    Input(component_id="down_sql_i_4", component_property="n_clicks")
)
def update_output_4(dropdown_selection_4,date_start_4,end_date_4,n_clicks_4):
    if n_clicks_4!=0:
        if dropdown_selection_4!=None:   
            dropdown_selection_4=str(dropdown_selection_4).replace('[','(').replace(']',')')
            sql=task_sql.table_sql_month_1.format(IdTask=dropdown_selection_4,DateTimeStart=date_start_4+'-01',DateTimeStop=end_date_4+'-01')
            
            df_table_sql_month_1=sql_con_django(sql=sql).applymap(str)
            sql=task_sql.table_sql_month_2.format(IdTask=dropdown_selection_4,DateTimeStart=date_start_4+'-01',DateTimeStop=end_date_4+'-01')
            #print(sql)
            df_table_sql_month_2=sql_con_django(sql=sql).applymap(str)
            df_table_sql_month=df_table_sql_month_2.merge(df_table_sql_month_1, on=['month','year'])
            df_table_sql_month=df_table_sql_month[['month', 'year','Максимальное время обслуживания вызова «АНТ», секунды',
       'Среднее время поствызывной обработки «ACW», секунды', 
       'Вызовы ВСЕГО', 'Вызовы принятые', '% Принятых',
       'Вызовы принятые за 20 сек. ', '% Принятых за 20 сек.',
       'Потерянные вызовы', '% Принятых', 'Среднее время разговора',
       'Среднее время ожидания отвеченных',
       'Среднее время ожидания потерянных',
       'Среднее время ожидания по всем звонкам ',
       'Максимальное время ожидания по всем звонкам',
       'Общее время разговора (трафик), минуты']]
            df_table_sql_month=df_table_sql_month.sort_values(by='month', ascending=False)
            return 0,date_start_4,end_date_4,html.Div([dash_table.DataTable(
            style_table={'overflowY': 'auto',},
            id='table_4',
            columns=[{"name": i, "id": i} for i in df_table_sql_month.columns],
            page_size=20,
            export_format='xlsx',
            export_headers='display',
            sort_action='native',
            data=df_table_sql_month.to_dict('records'))])
    elif n_clicks_4 is None:
        return 0,date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),date(int(datetime.datetime.now().strftime("%Y")),int(datetime.datetime.now().strftime("%m")),int(datetime.datetime.now().strftime("%d"))),''       
    raise PreventUpdate 