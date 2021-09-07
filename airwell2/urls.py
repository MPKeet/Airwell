"""airwell2 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from fileparse import views

urlpatterns = [
    path('', views.Home.as_view(), name='home'),
    path('admin/', admin.site.urls),
    path('upload_yaml/', views.upload, name='upload_yaml'),
    path('upload_extras/', views.extra_uploads, name='extra_uploads'),
    path('result_file/', views.viewer, name='result_file'),
    path('result_file/actionUrl/', views.download_dag, name='result_file/actionUrl'),
]
