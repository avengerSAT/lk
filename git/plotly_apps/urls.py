from django.conf import settings
from django.conf.urls.static import static

from django.contrib import admin
from django.urls import path, include,re_path
from django.contrib.auth import views as auth_views
from . import dash_otchet_user_state
from . import lk0
from . import lk1
from . import lk_vhod_0
from . import views


urlpatterns = [
    path('otchet_state', views.otchet_state.as_view(),name='url_otchet_state'),
    path('lk1', views.lk1.as_view(),name='lk1_url'),
    path('lk0', views.lk0.as_view(),name='lk0_url'),
    path('lk_vhod_0', views.lk_vhod_0.as_view(),name='lk_vhod_0_url'),
    path('frame', views.frame.as_view(),name='frame_url'),
#    path('django_plotly_dash/app/swiftdrive_lk/static/assets/<str:audio_read_text>/', views.audio_read, name='audio_read'),
    path('sms_p/<str:audio_read_text>/', views.audio_read, name='audio_read_url'),
    path('django_plotly_dash/', include('django_plotly_dash.urls')),
    
    ]+ static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

