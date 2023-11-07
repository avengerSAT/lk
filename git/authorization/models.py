from django.db import models
from django.shortcuts import reverse
from django.contrib.auth.models import User

class user_access(models.Model):
    user_id= models.ForeignKey(User, on_delete=models.CASCADE)
    access_lvl= models.IntegerField()
    def __str__(self):
        return '{}'.format(self.user_id)
    
class user_urls(models.Model):
    user_id= models.ForeignKey(User, on_delete=models.CASCADE)
    user_urls= models.TextField()
    def __str__(self):
        return '{}'.format(self.user_id)    