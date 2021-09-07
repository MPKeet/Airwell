
from django.db import models

class UploadYaml(models.Model):
    configs = models.FileField(upload_to='fileparse/generator/dag_files/')
class UploadExtras(models.Model):
    files = models.FileField(upload_to='fileparse/generator/dag_files/')

