from importlib import import_module
import yaml
import sys
import ast
import inspect
import os
import sys
import importlib.util
import pkgutil

import distutils.sysconfig as sysconfig


from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from yaml import loader
from collections import defaultdict
from dill.source import getsource

from .email_logic import email_logic
from .bash_logic import bash_logic
from .snowflake_logic import snowflake_logic

sys.path.append("./dag_files/")


class dag_generation_v1:
    def __init__(self, yaml_config):
        CONFIG_FILE = yaml_config
        path = Path(__file__).parent
        path = Path(path, "./dag_files/{}".format(CONFIG_FILE))
        try:
            with path.open() as fp:
                self.config = yaml.load(fp, Loader=yaml.Loader)
            self.dag_id = self.config["DAG_SETTINGS"]["dag_id"]
            self.dag_schedule = self.config["DAG_SETTINGS"]["dag_schedule"]
            self.support_email = self.config["DAG_SETTINGS"]["support_email"]
            self.description = self.config["DAG_SETTINGS"]["description"]

        except FileNotFoundError:
            print("config file not found in generation folder")
            print(path)
        except KeyError:
            print("required item in DAG_SETTINGS not found")

    def task_master(self):
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
                        self.script_parsing()
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
                            self.bash_task = bash_logic(block_name, order, file=file)
                        elif "command" in self.blocks and "file" not in self.blocks:
                            command = str(self.blocks["command"])
                            self.bash_task = bash_logic(
                                block_name, order, command=command
                            )
                        elif "command" and "file" in self.blocks:
                            command = str(self.blocks["command"])
                            file = str(self.blocks["file"])
                            self.bash_task = bash_logic(
                                block_name, order, file=file, command=command
                            )
                        else:
                            print(
                                "missing either filepath or command associated with {}".format(
                                    block_name
                                )
                            )
                    except KeyError:
                        print(
                            "missing filepath or command associated with {}".format(
                                block_name
                            )
                        )
                        return "missing filepath or command associated with {}".format(
                            block_name
                        )
                elif self.type == "email_operator":
                    try:
                        subject = self.blocks["subject"]
                        email_to = self.blocks["email_to"]
                        order = self.blocks["order"]
                        text = self.blocks["text"]
                        self.email_block_name = block_name
                        self.email_order = order

                        self.email_task = email_logic(
                            block_name, email_to, subject, text, order
                        )
                    except KeyError:
                        print(
                            "missing email to or from associated with {}".format(
                                block_name
                            )
                        )
                        return "missing email to or from associated with {}".format(
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
                            self.sf_task = snowflake_logic(
                                block_name, order, connection, file=file
                            )
                        elif "command" in self.blocks and "file" not in self.blocks:
                            command = str(self.blocks["command"])
                            self.sf_task = snowflake_logic(
                                block_name, order, connection, command=command
                            )
                        elif "command" and "file" in self.blocks:
                            command = str(self.blocks["command"])
                            file = str(self.blocks["file"])
                            self.sf_task = snowflake_logic(
                                block_name,
                                order,
                                connection,
                                file=file,
                                command=command,
                            )
                        else:
                            print(
                                "missing either filepath or command associated with {}".format(
                                    block_name
                                )
                            )

                    except KeyError:
                        print("missing filepath associated with {}".format(block_name))
                        return "missing filepath associated with {}".format(block_name)
                else:
                    print("not a valid task type")

            except KeyError:
                print("missing task name or type associated with {}".format(block_name))
                return "missing task name or type associated with {}".format(block_name)

        self.task_ordering()
        self.template_write()

    def script_parsing(self):

        parent_path = Path(__file__).parent
        file_path = Path(parent_path, "./dag_files/{}".format(self.file))

        source = open(file_path).read()
        functions = [
            f.name for f in ast.parse(source).body if isinstance(f, ast.FunctionDef)
        ]

        standard_dict = []
        std_lib = sysconfig.get_python_lib(standard_lib=True)
        for top, dirs, files in os.walk(std_lib):
            for nm in files:
                if nm != "__init__.py" and nm[-3:] == ".py":
                    standard_dict.append(
                        os.path.join(top, nm)[len(std_lib) + 1 : -3].replace(
                            os.sep, "."
                        )
                    )

        if functions:
            funcs = ""
            count = -1
            for function in functions:
                requirements = []
                count += 1
                func_deps = ""

                string_mod_absolute = str(self.file).split(".")[0]

                dirname = "./fileparse/generator/dag_files/"

                for importer, package_name, _ in pkgutil.iter_modules([dirname]):
                    full_package_name = "%s.%s" % (dirname, package_name)
                    if full_package_name not in sys.modules:
                        module = importer.find_module(package_name).load_module(
                            package_name
                        )

                func = getattr(__import__(string_mod_absolute), function)

                code_lines = inspect.getsource(func)

                with open(file_path, "rt") as file:
                    for line in file.readlines():
                        if "import" == line.split(" ")[0]:
                            func_deps += "    " + line
                            if "," in line:
                                deps = line.split(" ")[1:]
                                for dep in deps:
                                    dep = dep.replace(",", "")
                                    if dep in standard_dict:
                                        pass
                                    else:
                                        requirements.append(dep.strip("\n"))
                            elif "," not in line:
                                if (line.split(" ")[1]).strip("\n") in standard_dict:
                                    pass
                                else:
                                    requirements.append(
                                        (line.split(" ")[1]).strip("\n")
                                    )
                        elif "from" == line.split(" ")[0]:
                            func_deps += "    " + line
                            if (line.split(" ")[1]).strip("\n") in standard_dict:
                                pass
                            else:
                                requirements.append((line.split(" ")[1]).strip("\n"))
                        else:
                            pass

                mod_code_lines = code_lines.split("\n")[0] + "\n\n" + func_deps
                mod_code_lines += code_lines.split(":", 1)[1]
                funcs += mod_code_lines + "\n\n"

                self.order_dict[function] = self.config["BLOCKS"][self.block_name][
                    "order"
                ][count]
                self.dep_dict[function] = requirements

            self.cumulative_code += funcs

        if not functions:
            requirements = []
            func_deps = ""
            mimic_func = "def {}():\n\n".format(self.block_name)
            with open(file_path, "rt") as file:
                for line in file.readlines():
                    mimic_func += "    " + line

                    if "import" == line.split(" ")[0]:
                        func_deps += "    " + line
                        if "," in line:
                            deps = line.split(" ")[1:]
                            for dep in deps:
                                dep = dep.replace(",", "")
                                if dep in standard_dict:
                                    pass
                                else:
                                    requirements.append(dep.strip("\n"))
                        elif "," not in line:
                            if (line.split(" ")[1]).strip("\n") in standard_dict:
                                pass
                            else:
                                requirements.append((line.split(" ")[1]).strip("\n"))
                    elif "from" == line.split(" ")[0]:
                        func_deps += "    " + line
                        if (line.split(" ")[1]).strip("\n") in standard_dict:
                            pass
                        else:
                            requirements.append((line.split(" ")[1]).strip("\n"))
                    else:
                        pass

            self.dep_dict[self.block_name] = requirements

            self.cumulative_code += mimic_func + "\n\n"
            self.order_dict[self.block_name] = self.config["BLOCKS"][self.block_name][
                "order"
            ]

    def task_ordering(self):
        # python task ordering
        os.chdir(os.path.dirname(__file__))
        env = Environment(loader=FileSystemLoader(searchpath="./templates"))
        template = env.get_template("virtualenv_task.j2")
        task_num = 0
        for key, value in self.order_dict.items():
            task_num += 1
            tasks = template.render(
                task_id="pyVenv{}".format(task_num),
                order=value,
                callable=key,
                requirements=self.dep_dict[key],
            )
            task_id = "pyVenv{}".format(task_num)
            self.task_string += str(tasks + "\n\n")
            self.task_func_match[key] = task_id

        sorted_funcs = sorted(self.order_dict.items(), key=lambda x: x[1])

        # setting task to order defined in order_dict
        for val in sorted_funcs:
            task_name = self.task_func_match.get(val[0])
            if task_name != None:
                self.task_sequence[task_name] = val[1]
            else:
                raise ValueError("task name / function name is returning as none")

        # inserting bash task to ordering
        if "bash_order" in dir(self):
            self.task_sequence[self.bash_block_name] = self.bash_order
        # inserting email task to ordering
        if "email_order" in dir(self):
            self.task_sequence[self.email_block_name] = self.email_order
        # inserting sf task to ordering
        if "sf_order" in dir(self):
            self.task_sequence[self.sf_block_name] = self.sf_order

        sequence_string = ""
        res = defaultdict(list)

        for task_name, order in sorted(self.task_sequence.items()):
            res[order].append(task_name)
        for index, val in sorted(res.items()):
            if len(val) == 1:
                val = str(val).replace("[", "").replace("]", "").replace("'", "")
            sequence_string += "{} >> ".format(str(val).replace("'", ""))
        size = len(sequence_string)
        self.sequence_string = str(sequence_string[: size - 4])

    def template_write(self):
        # Email function Insert
        if "email_order" in dir(self):
            self.task_string += str(self.email_task + "\n\n")
        # Bash Function Insert
        if "bash_order" in dir(self):
            self.task_string += str(self.bash_task + "\n\n")
        # Sf task insert
        if "sf_order" in dir(self):
            self.task_string += str(self.sf_task + "\n\n")

        os.chdir(os.path.dirname(__file__))
        env = Environment(loader=FileSystemLoader(searchpath="./templates"))

        template = env.get_template("dag.j2")
        file = open("resultFile/" + self.dag_id + "_dag.py", "w")
        file.write(
            template.render(
                description=self.description,
                functions=self.cumulative_code,
                support_email=self.support_email,
                ordering=self.task_string,
                sequence=self.sequence_string,
            )
        )

        file.close()
