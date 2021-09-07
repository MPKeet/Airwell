
import os
import time
import mimetypes

from pathlib import Path
from django.apps import config
from django.core.files import uploadedfile, File
from django.http.response import HttpResponse
from django.shortcuts import render, redirect
from django.views.generic import TemplateView, ListView, CreateView
from django.core.files.storage import FileSystemStorage
from django.urls import conf, reverse_lazy
from django.views.generic.edit import FormView
from django.views.static import serve

from .models import UploadExtras
from .forms import ConfigUpload, ExtrasUpload
from .generator.generation import dag_generation_v1
from .generator.file_count import file_count





class Home(TemplateView):
    template_name = 'home.html'


def block_sort(yaml_config):
    file_list=file_count(yaml_config)
    file_list=file_list.task_master()
    return file_list


def upload(request):
    if request.method == 'POST':
        config_path = Path(__file__).parent
        config_path = Path( config_path , './generator/dag_files/')
        file_list=os.listdir(config_path)
        
        for file in file_list:
            if file != "__init__.py" and file != "__pycache__":
                file_path = Path(__file__).parent
                file_path = Path( file_path , './generator/dag_files/{}'.format(file))
                os.remove(file_path)
            else:
                pass
        extras_path = Path(__file__).parent
        extras_path = Path( extras_path , './generator/resultFile/')
        dag_list=os.listdir(extras_path)
        
        for file in dag_list:
            if file != "__init__.py" and file != "__pycache__":
                extras_path = Path(__file__).parent
                extras_path = Path( extras_path , './generator/resultFile/{}'.format(file))
                os.remove(extras_path)
            else:
                pass

        form = ConfigUpload(request.POST, request.FILES)
        yaml_config_name, yaml_config = str(request.FILES['configs']), request.FILES['configs']
        if "(application/x-yaml)" in str(request.FILES):
            if form.is_valid():
                path = Path(__file__).parent
                path = Path( path , './generator/dag_files/{}'.format(yaml_config_name))
                with open(path, 'wb+') as destination:
                    for chunk in yaml_config.chunks():
                        destination.write(chunk)
                request.session["file_list"]=block_sort(yaml_config)
                request.session["yaml_config_name"]=yaml_config_name
                context = {'msg' : '<span style="color: green;">File successfully uploaded</span>'}
                return redirect('/upload_extras/')

        
    else:
        form = ConfigUpload()
    return render(request, 'upload.html', {'form':form})


def extra_uploads(request):
    if request.method == 'POST':
        form = ExtrasUpload(request.POST, request.FILES)
        files = request.FILES.getlist('files')
        if form.is_valid():

            file_list = request.session.get("file_list", "[]")
            upload_list=[]
            for f in files:
                upload_list.append(f.name)
                path = Path(__file__).parent
                path = Path( path , './generator/dag_files/{}'.format(f.name))
                with open(path, 'wb+') as destination:
                    for chunk in f.chunks():
                        destination.write(chunk)

            path = Path(__file__).parent
            path = Path( path , "./generator/dag_files/")
            uploaded_files=os.listdir(path)

            for file in file_list:
                if file in upload_list or file in uploaded_files:
                    context = {'msg' : '<span style="color: green;">File successfully uploaded</span>'}
                    pass
                else:
                    context = {'msg' : '<span style="color: red;">Based on uploaded config you are missing file {}</span>'.format(file)}
                    return render(request, 'upload_extras.html', context)
                    break
            if context == {'msg' : '<span style="color: green;">File successfully uploaded</span>'}:
                yaml_config = request.session.get("yaml_config_name", "")
                dagger=dag_generation_v1(yaml_config)
                dagger.task_master()

                return redirect('/result_file/')
    else:
        form = ExtrasUpload()
    return render(request, 'upload_extras.html', {'form':form})   

def viewer(request):
    path = Path(__file__).parent
    path = Path( path , "./generator/resultFile/")
    dag_list = os.listdir(path)
    if len(dag_list) > 0:
        path = Path( path , "./{}".format(dag_list[0]))
        f = open(path)
        file_contents = f.read()
        f.close()
        args = {'dag': file_contents}

        return render(request, "dag_render.html", args)
    else:
        context = {'msg' : '<span style="color: red;">Proper files have not been uploaded, please reupload your docs </span>'}
        return render(request, 'home.html', context)


def download_dag(request):
    if request.method=='GET':
        path = Path(__file__).parent
        path = Path( path , "./generator/resultFile/")
        dag_list = os.listdir(path)
        if len(dag_list) > 0:
            print("yes")
            dag_name=dag_list[0]
            path = Path( path , "./{}".format(str(dag_name)))
            f = open(path, 'rb')
            mime_type, _ = mimetypes.guess_type(path)
            response = HttpResponse(f, content_type=mime_type)
            response['Content-Disposition'] = 'attachment; filename=%s' % str(dag_name)
            f.close()
            return response
        else:
            context = {'msg' : '<span style="color: red;">Proper files have not been uploaded, please reupload your docs </span>'}
            return render(request, 'home.html', context)

def config_remove(yaml_config):
    path = Path(__file__).parent
    path = Path( path , './generator/dag_files/{}'.format(yaml_config))
    time.sleep(15)
    os.remove(path)
    
    