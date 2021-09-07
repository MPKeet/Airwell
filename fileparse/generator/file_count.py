import yaml
import sys
import ast
import inspect
import os
import sys

import distutils.sysconfig as sysconfig

from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from yaml import loader
from collections import defaultdict
from .email_logic import email_logic
from .bash_logic import bash_logic
from .snowflake_logic import snowflake_logic


class file_count:
    def __init__(self, yaml_config):

        CONFIG_FILE = yaml_config
        self.path = Path(__file__).parent
        self.path = Path(self.path, "./dag_files/{}".format(CONFIG_FILE))

        try:
            with self.path.open() as fp:
                self.config = yaml.load(fp, Loader=yaml.Loader)
            self.dag_id = self.config["DAG_SETTINGS"]["dag_id"]
            self.dag_schedule = self.config["DAG_SETTINGS"]["dag_schedule"]
            self.support_email = self.config["DAG_SETTINGS"]["support_email"]
            self.description = self.config["DAG_SETTINGS"]["description"]
            self.task_master()
        except FileNotFoundError:
            print("config file not found in generation folder")
        except KeyError:
            print("required item in DAG_SETTINGS not found")

    def task_master(self):
        file_names = []
        self.cumulative_code = ""
        self.order_dict = {}
        self.task_string = ""
        self.dep_dict = {}
        self.task_func_match = {}
        self.task_sequence = {}
        for block_name in self.config["BLOCKS"].keys():
            try:
                self.block_name = block_name
                self.blocks = self.config["BLOCKS"][block_name]
                self.type = self.blocks["type"].lower()

                if self.type == "python":
                    try:
                        self.file = self.blocks["file"]
                        file_names.append(self.file)
                    except KeyError:
                        print("missing filepath associated with {}".format(block_name))
                        return "missing filepath associated with {}".format(block_name)
                elif self.type == "bash":
                    try:
                        order = self.blocks["order"]
                        self.bash_block_name = block_name
                        self.bash_order = self.blocks["order"]
                        if "file" in self.blocks and "command" not in self.blocks:
                            file = str(self.blocks["file"])
                            file_names.append(file)
                        elif "command" and "file" in self.blocks:

                            file = str(self.blocks["file"])
                            file_names.append(file)
                        else:
                            pass
                    except KeyError:
                        print(
                            "missing filepath or command associated with {}".format(
                                block_name
                            )
                        )
                        return "missing filepath or command associated with {}".format(
                            block_name
                        )
                elif self.type == "snowflake_query":
                    try:
                        order = self.blocks["order"]
                        connection = self.blocks["connection"]
                        self.sf_order = self.blocks["order"]
                        self.sf_block_name = block_name
                        self.sf_connection = self.blocks["connection"]

                        if "file" in self.blocks and "command" not in self.blocks:
                            file = str(self.blocks["file"])
                            file_names.append
                        elif "command" and "file" in self.blocks:
                            file = str(self.blocks["file"])
                            file_names = [file]
                        else:
                            pass

                    except KeyError:
                        print("missing filepath associated with {}".format(block_name))
                        return "missing filepath associated with {}".format(block_name)
                else:
                    print("not a valid task type")

            except TypeError:
                print("type error within {}".format(block_name))
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                return exc_type, fname, exc_tb.tb_lineno, " with {}".format(block_name)
            except KeyError:
                print("missing task name or type associated with {}".format(block_name))
                return "missing task name or type associated with {}".format(block_name)
        return file_names
