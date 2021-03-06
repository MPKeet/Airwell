# Generated by Django 3.2.6 on 2021-09-03 03:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('fileparse', '0003_auto_20210901_1857'),
    ]

    operations = [
        migrations.CreateModel(
            name='UploadExtras',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('extras', models.FileField(upload_to='fileparse/generator/dag_files/')),
            ],
        ),
        migrations.AlterField(
            model_name='uploadyaml',
            name='configs',
            field=models.FileField(upload_to='fileparse/generator/dag_files/'),
        ),
    ]
