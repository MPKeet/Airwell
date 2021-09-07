import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from yaml import loader


def bash_logic(block_name, order, **kwargs):
    os.chdir(os.path.dirname(__file__))
    env = Environment(loader=FileSystemLoader(searchpath="./templates"))
    template = env.get_template("bash_task.j2")
    if "file" in kwargs and "command" not in kwargs:
        command = "./" + str(kwargs.get("file"))
    elif "command" in kwargs and "file" not in kwargs:
        command = str(kwargs.get("command"))
    elif "command" and "file" in kwargs:
        command = (
            str(kwargs.get("command")) + " && " + "./" + str(kwargs.get("file")) + " "
        )
    else:
        return "ensure that you have either a file or command in your gener file"

    tasks = template.render(task_id=block_name, command=command)
    return tasks
