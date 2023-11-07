from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.contrib import messages
from django.contrib.auth import login as auth_login, authenticate, logout
from .models import *
from .forms import LoginForm


def sign_in(request):

    if request.method == 'GET':
        form = LoginForm()
        return render(request,'users/login.html', {'form': form})
    
    elif request.method == 'POST':
        form = LoginForm(request.POST)
        
        if form.is_valid():
            username = form.cleaned_data['username']
            password=form.cleaned_data['password']
            user = authenticate(request,username=username,password=password)
            if user:
                auth_login(request, user)
                
                urls=user_urls.objects.filter(user_id=request.user.id)
                if urls.exists():
                    urls=(urls.values()[0])['user_urls']
                    
                    if str(urls).find(',') >=0:
                        urls=str(urls).split(',') 
                        return render (request,'menu.html',{'data':urls})
                    else:
                        return redirect(urls)
                else:
                    messages.success(request,f'Hi {username.title()}, welcome back!')
                    return HttpResponse('Авторезован')
        
        # either form not valid or user is not authenticated
        messages.error(request,f'Invalid username or password')
        return render(request,'users/login.html',{'form': form})

def sign_out(request):
    logout(request)
    messages.success(request,f'You have been logged out.')
    return redirect('login')     


class swift_sl(LoginRequiredMixin, View):
    def get(self,request):
        return redirect('swiftdrive_url')  

    def post(self,request):
        return redirect('swiftdrive_url')  