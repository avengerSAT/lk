task_sql='''select 
id,  Name
from oktell_settings.dbo.A_TaskManager_Tasks qq
WHERE 1=1
and qq.Name like 'СТС%'
and IdProject ='1B7C42CB-CE85-4B26-9943-76B1AE7CA3EE'
'''

table_sql_otchet='''
SELECT 
COUNT(*) as [Вызовы ВСЕГО],
sum(
        case when CallResult in (5,8)  then 1 else 0 end
) as [Вызовы принятые] ,
case when sum(case when CallResult in (5,8) then 1 else 0 end)!=0
then 
ROUND( CAST(sum(
        case when CallResult in (5,8)   then 1 else 0 end
)AS FLOAT)*100/COUNT(*),1) 
else 0
end [% Принятых],
case when sum(case when CallResult in (5,8)   and LenQueue<=20 then 1 else 0 end)!=0
then
sum(case when CallResult in (5,8)   and LenQueue<=20 then 1 else 0 end) 
else 0
end [Вызовы принятые за 20 сек. ],
case 
    when sum(case when CallResult in (5,8)   and LenQueue<=20 then 1 else 0 end)!=0
then
ROUND( CAST(sum(case when CallResult in (5,8)   and LenQueue<=20 then 1 else 0 end)AS FLOAT)*100/COUNT(*),1)  
else 0
end [% Принятых за 20 сек.],
case when COUNT(*)-sum(case when CallResult in (5,8)   then 1 else 0 end)  !=0
then
COUNT(*)-sum(case when CallResult in (5,8)   then 1 else 0 end) 
else 0
end  [Потерянные вызовы],
case 
    when  COUNT(*)-sum(case when CallResult in (5,8)   then 1 else 0 end)!=0
then ROUND( CAST(COUNT(*)-sum(case when CallResult in (5,8)   then 1 else 0 end)AS FLOAT)*100/COUNT(*),1)
else 0
end [% Принятых],
case 
    when sum(LenTime)!=0 and sum(case when CallResult in (5,8)  then 1 else 0 end)!=0 
then CONVERT(time(0), DATEADD(SECOND,ROUND (ROUND(sum ( LenTime)/ sum(
        case when CallResult in (5,8)  then 1 else 0 end
),1),1), 0)) 
else CONVERT(time(0), DATEADD(SECOND,ROUND (0.0,1), 0)) 
end [Среднее время разговора],
case 
    when sum(LenQueue)!=0 and sum(case when CallResult in (5,8)  then 1 else 0 end)!=0
then CONVERT(time(0), DATEADD(SECOND,ROUND (ROUND(sum (LenQueue )/ sum(
       case when CallResult in (5,8)  then 1 else 0 end
),1),1), 0)) 
else CONVERT(time(0), DATEADD(SECOND,ROUND (0.0,1), 0)) 
end [Среднее время ожидания отвеченных],
case 
    when sum(LenQueue)!=0 and sum(case when CallResult  not in (5,8) then 1 else 0 end)!=0
then CONVERT(time(0), DATEADD(SECOND,ROUND (ROUND(sum ( LenQueue )/ sum(
        case when CallResult  not in (5,8) then 1 else 0 end
),1),1), 0)) 
else CONVERT(time(0), DATEADD(SECOND,ROUND (0.0,1), 0)) 
end  [Среднее время ожидания потерянных],
CONVERT(time(0), DATEADD(SECOND,ROUND (ROUND(sum ( LenQueue )/count(*),1),1), 0)) as [Среднее время ожидания по всем звонкам ],
CONVERT(time(0), DATEADD(SECOND,ROUND (ROUND(max(LenQueue),1),1), 0))  as [Максимальное время ожидания по всем звонкам]
from (SELECT 
IdEffort
,sum(case when IdOperator !='AB000000-0000-0000-0000-000000000000' and IdOperator is not null THEN LenTime else 0 end) as LenTime
,sum(case when IdOperator is null THEN LenQueue else 0 end) as LenQueue
,CallResult
from oktell_cc_temp.dbo.A_Cube_CC_EffortConnections
WHERE 1=1
and IdTask in {IdTask}
and convert (date,DateTimeStart) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}')
GROUP by IdEffort ,CallResult  ) qwe
'''

table_sql_month_1='''
SELECT 
[month],
[year],
COUNT(*) as [Вызовы ВСЕГО],
sum(
    case when CallResult in (5,8)  then 1 else 0 end
) as [Вызовы принятые] ,
ROUND( CAST(sum(
    case when CallResult in (5,8)   then 1 else 0 end
)AS FLOAT)*100/COUNT(*),1) as [% Принятых] ,
sum(
    case when CallResult in (5,8)   and LenQueue<=20 then 1 else 0 end
) as [Вызовы принятые за 20 сек. ],
ROUND( CAST(sum(
    case when CallResult in (5,8)   and LenQueue<=20 then 1 else 0 end
)AS FLOAT)*100/COUNT(*),1) as [% Принятых за 20 сек.],
COUNT(*)-sum(
    case when CallResult in (5,8)   then 1 else 0 end
) as [Потерянные вызовы],
ROUND( CAST(COUNT(*)-sum(
    case when CallResult in (5,8)   then 1 else 0 end
)AS FLOAT)*100/COUNT(*),1) as [% Принятых],
ROUND(sum ( LenTime)/ sum(
    case when CallResult in (5,8)  then 1 else 0 end
),1) as [Среднее время разговора],
ROUND(sum (LenQueue )/ sum(
    case when CallResult in (5,8)  then 1 else 0 end
),1)
 as [Среднее время ожидания отвеченных] ,
ROUND(sum ( LenQueue )/ sum(
    case when CallResult  not in (5,8) then 1 else 0 end
),1) as [Среднее время ожидания потерянных],
ROUND(sum ( LenQueue )/count(*),1) as [Среднее время ожидания по всем звонкам ],
ROUND(max(LenQueue),1) as [Максимальное время ожидания по всем звонкам],
ROUND(sum ( LenTime)/60,1) as [Общее время разговора (трафик), минуты]
from (SELECT 
DATEPART (month,CONVERT (date,DateStart)) as [month],
DATEPART (year,CONVERT (date,DateStart)) as [year],
IdEffort
,sum(case when IdOperator !='AB000000-0000-0000-0000-000000000000' and IdOperator is not null THEN LenTime else 0 end) as LenTime
,sum(case when IdOperator is null THEN LenQueue else 0 end) as LenQueue
,CallResult
from oktell_cc_temp.dbo.A_Cube_CC_EffortConnections
WHERE 1=1
and IdTask in {IdTask}
and DATEPART (month,convert (date,DateTimeStart)) BETWEEN  DATEPART (month,convert (date,'{DateTimeStart}')) and DATEPART (month,convert (date,'{DateTimeStop}'))
GROUP by IdEffort ,CallResult,CONVERT (date,DateStart)  ) qwe
GROUP by [month],[year]
'''

table_sql_month_2='''
SELECT 
[month],[year],
ROUND(max (LenTime),1) as [Максимальное время обслуживания вызова «АНТ», секунды],
ROUND(sum([Поствызывная обработка])/COUNT(*),1)  as [Среднее время поствызывной обработки «ACW», секунды]
from (select 
DATEPART (month,CONVERT (date,DateStart)) as [month],
DATEPART (year,CONVERT (date,DateStart)) as [year],
sum(LenTime) as LenTime ,IdEffort
,sum(case when state=7 then LenTime else 0 end  ) as [Поствызывная обработка] 
from oktell_cc_temp.dbo.A_Cube_CC_OperatorStates
WHERE 1=1
and IdEffort in (select IdEffort from oktell_cc_temp.dbo.A_Cube_CC_EffortConnections
WHERE 1=1
and IdTask in {IdTask}
and DATEPART (month,convert (date,DateTimeStart)) BETWEEN  DATEPART (month,convert (date,'{DateTimeStart}')) and DATEPART (month,convert (date,'{DateTimeStop}')))
and IdOperator is not null
and CallResult=5
GROUP by IdEffort  ,CONVERT (date,DateStart)
) qwe
GROUP by [month],[year]
'''

sql_vhod_otchet_1='''SELECT 
date_call as [Дата звонка/Время звонка]
,IdComChain
,Info as [Номер очереди/наименование очереди]
,r21 as [Номер оператора/ФИО оператора]
,AbonentNumber as [АОН клиента]
,R1 as 'Суть обращения:'
,R2 as 'Результат звонка: '
,R3 as 'Звонок поступил на номер:'
,R4 as 'ФИО Абонента: '
,R5 as  'Наименование организации абонента:'
,R6 as  'Контактный телефон абонента: '
,R7 as 'Интересующий вопрос: '
,r8 as [Номер перевода (если был)]
,r9 as [Статус перевода на внутренний номер]
,r10 as [Статус маршрутизации]
,r11 as [Направление]
from oktell.dbo.s081_Qverty_In
WHERE 
Task_Id in {Task_Id}
and convert (date,date_call) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}')
'''
sql_vhod_otchet_1_2='''SELECT 
--date_call as [Дата звонка/Время звонка],
IdComChain,IdEffort
,r21 as [Номер оператора/ФИО оператора]
,R1 as 'Суть обращения:'
,R2 as 'Результат звонка: '
,R3 as 'Звонок поступил на номер:'
,R4 as 'ФИО Абонента: '
,R5 as  'Наименование организации абонента:'
,R6 as  'Контактный телефон абонента: '
,R7 as 'Интересующий вопрос: '
,r8 as [Номер перевода (если был)]
,r9 as [Статус перевода на внутренний номер]
,r10 as [Статус маршрутизации]
,r11 as [Направление]
from oktell.dbo.s081_Qverty_In q
inner join oktell_cc_temp.dbo.A_Cube_CC_EffortConnections w on  q.IdComChain=w.IdChain 
WHERE 
Task_Id in {Task_Id}
and convert (date,date_call) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}')
group by IdComChain ,r21,r1,r2,r3,r4,r6,r7,r8,r5,r9,r10,r11,IdEffort
'''
sql_vhod_otchet_2='''
SELECT 
ROUND(sum(case when State=3 then LenTime else 0 end),1) as [Обратный вызов]
,ROUND(sum(case when State=6 then LenTime else 0 end),1) as [Разговор по задаче]
,ROUND(ROUND(sum(case when State=6 then LenTime else 0 end),1)/60,1) as [Разговор по задаче в мин.]
,ROUND(sum(case when State=7 then LenTime else 0 end),1) as [Поствызывная обработка]
,IdEffort 
from oktell_cc_temp.dbo.A_Cube_CC_OperatorStates
WHERE IdTask in {Task_Id}
and convert (date,DateTimeStart) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}')
GROUP by IdEffort
'''
sql_vhod_otchet_2_2='''
SELECT 
 CONVERT(time(0), DATEADD(SECOND,ROUND(sum(case when State=3 then LenTime else 0 end),1), 0))  as [Обратный вызов]
,CONVERT(time(0), DATEADD(SECOND,ROUND(sum(case when State=6 then LenTime else 0 end),1), 0))  as [Разговор по задаче]
,CONVERT(time(0), DATEADD(SECOND,ROUND(sum(case when State=7 then LenTime else 0 end),1), 0))  as [Поствызывная обработка]
,IdEffort 
from oktell_cc_temp.dbo.A_Cube_CC_OperatorStates
WHERE IdTask in {Task_Id}
and convert (date,DateTimeStart) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}')
GROUP by IdEffort
'''

sql_vhod_otchet_3='''
SELECT 
IdEffort,IdChain as IdComChain,ROUND (LenQueue,1) as [Время ожидания]
FROM oktell_cc_temp.dbo.A_Cube_CC_EffortConnections
WHERE 
IdChain in (select IdComChain from oktell.dbo.s081_Qverty_In
WHERE 
Task_Id in {Task_Id}
and convert (date,date_call) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}'))
GROUP by IdChain ,IdEffort,LenQueue 
'''
sql_vhod_otchet_3_2='''
SELECT 
min(DateTimeStart) as [Дата звонка/Время звонка] ,AbonentNumber as [АОН клиента],
IdEffort ,
CONVERT(time(0), DATEADD(SECOND,ROUND (LenQueue,1), 0))  as [Время ожидания],IdTask as  [Номер очереди/наименование очереди]
FROM oktell_cc_temp.dbo.A_Cube_CC_EffortConnections
WHERE 
IdTask in {Task_Id}
and convert (date,DateTimeStart) BETWEEN  convert (date,'{DateTimeStart}') and convert (date,'{DateTimeStop}')
GROUP by IdEffort,LenQueue ,AbonentNumber,IdTask 
'''

sql_vhod_otchet_4_2='''
SELECT 
r3,Task_Id
from oktell.dbo.s081_Qverty_In
WHERE Task_Id in {Task_Id}
and r3 is not null
GROUP by r3,Task_Id
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
and IdTask in {Task_Id}
and (q.IdOperator is not NULL  and q.IdOperator!='AB000000-0000-0000-0000-000000000000')
and IdConn is not NULL GROUP by IdConn )'''

