

def sql_con_django(sql):
    from django.db import connection
    import pandas as pd
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = list(cursor.fetchall())
        head = ([column[0] for column in cursor.description]) 
        data = pd.DataFrame(data,columns=head)   
    return data        