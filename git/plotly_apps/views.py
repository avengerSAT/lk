from django.shortcuts import render
from django.http import HttpResponse
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.db import connection
import pandas as pd
from authorization.models import *

sql='''
       select 
    name
    from oktell_settings.dbo.A_Users
    where
    id='90475a1f-04a8-41e1-9440-7d1aaeabea01'
'''

def proverka(user_id):
    access=(user_access.objects.filter(user_id=user_id).values()[0])['access_lvl']
    urls=(user_urls.objects.filter(user_id=user_id).values()[0])['user_urls']
    if urls.find(',') >=0:
        urls=urls.split(',')
    else:
        urls=[urls]    
    return access,urls

def sql_con_django(sql):
    from django.db import connection
    import pandas as pd
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = list(cursor.fetchall())
        head = ([column[0] for column in cursor.description]) 
        data = pd.DataFrame(data,columns=head) 
    return data        

class otchet_state(LoginRequiredMixin, View):
    def get(self,request):
        return render (request,'plotly_apps/dash_otchet_user_state.html')

    def post(self,request): 
        return render (request,'plotly_apps/dash_otchet_user_state.html')

class frame(LoginRequiredMixin, View):
    def get(self,request):
        return render (request,'plotly_apps/frame.html')

    def post(self,request):
        return render (request,'plotly_apps/frame.html')
        
class lk1(LoginRequiredMixin, View):
    def get(self,request):
        access,urls=proverka(request.user.id)
        print(access)
        if access in (1,2,3):
            print(1)
            return render (request,'plotly_apps/lk1.html')
        print(urls)
        if 'swiftdrive_url' in urls:
            return render (request,'plotly_apps/lk1.html')
        qwe=[]
        if str(urls).find(',') >=0:
            urls=str(urls).split(',') 
        return render (request,'menu.html',{'data':urls})


    def post(self,request):
        access,urls=proverka(request.user.id)
        if access in (1,2,3):
            return render (request,'plotly_apps/lk1.html')
        print(urls)
        if 'swiftdrive_url' in urls:
            return render (request,'plotly_apps/lk1.html')
        qwe=[]
        if str(urls).find(',') >=0:
            urls=str(urls).split(',') 
        return render (request,'menu.html',{'data':urls})

    
class lk0(LoginRequiredMixin, View):
    def get(self,request):
        access,urls=proverka(request.user.id)
        if access in (1,2,3):
            return render (request,'plotly_apps/lk0.html')
        print(urls)
        if 'baltika_url' in urls:
            return render (request,'plotly_apps/lk0.html')
        qwe=[]
        if str(urls).find(',') >=0:
            urls=str(urls).split(',') 
        return render (request,'menu.html',{'data':urls})

    def post(self,request):
        access,urls=proverka(request.user.id)
        if access in (1,2,3):
            return render (request,'plotly_apps/lk0.html')
        if 'baltika_url' in urls:
            return render (request,'plotly_apps/lk0.html')
        qwe=[]
        if str(urls).find(',') >=0:
            urls=str(urls).split(',') 
        return render (request,'menu.html',{'data':urls})


def audio_read(request,audio_read_text):
    audio_read_text='''assets/{audio_read_text}'''.format(audio_read_text=audio_read_text)
    print(audio_read_text)
    return render (request,'plotly_apps/audio_read.html',{'audio_pah':audio_read_text})


class lk_vhod_0(LoginRequiredMixin, View):
    def get(self,request):
        access,urls=proverka(request.user.id)
        if access in (1,2,3):
            return render (request,'plotly_apps/lk_vhod_0.html')
        print(urls)
        if 'lk_vhod_0_url' in urls:
            return render (request,'plotly_apps/lk_vhod_0.html')
        qwe=[]
        if str(urls).find(',') >=0:
            urls=str(urls).split(',') 
        return render (request,'menu.html',{'data':urls})

    def post(self,request):
        access,urls=proverka(request.user.id)
        if access in (1,2,3):
            return render (request,'plotly_apps/lk_vhod_0.html')
        if 'lk_vhod_0_url' in urls:
            return render (request,'plotly_apps/lk_vhod_0.html')
        qwe=[]
        if str(urls).find(',') >=0:
            urls=str(urls).split(',') 
        return render (request,'menu.html',{'data':urls})