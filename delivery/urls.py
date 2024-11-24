
from django.contrib import admin
from django.urls import path
from home.views import *

urlpatterns = [
    path('', index),
    path('data/',get_data),
    path('admin/', admin.site.urls),

]
