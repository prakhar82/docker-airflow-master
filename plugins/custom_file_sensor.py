import os
import re
import fnmatch

from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator

class CustomFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, filepattern, *args, **kwargs):
        super(CustomFileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern

    def poke(self, context):
        full_path = self.filepath
        file_pattern = self.filepattern

        directory = os.listdir(full_path)

        for files in directory:
            if not (fnmatch.fnmatch(files, file_pattern)):
                print('not matching files ',files)
            else:
                context['task_instance'].xcom_push('file_name', files)
                return True
        return False

class CustomFileSensorPlugin(AirflowPlugin):
    name = "custom_file_sensor_plugin"
    operators = [CustomFileSensor]