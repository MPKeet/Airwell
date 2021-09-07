
from django import forms
from django.forms import ClearableFileInput, widgets
from .models import UploadYaml, UploadExtras

class ConfigUpload(forms.ModelForm):
    class Meta:
        model = UploadYaml
        fields = ('configs',)
        widgets = {
            'configs': ClearableFileInput(attrs={'multiple': True}),
        }


class ExtrasUpload(forms.ModelForm):
    class Meta:
        model =UploadExtras
        fields = ('files',)
        files = forms.FileField(widget=forms.ClearableFileInput(attrs={'multiple': True}))