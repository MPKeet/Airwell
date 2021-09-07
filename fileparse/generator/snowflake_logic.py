import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from yaml import loader


def snowflake_logic(block_name, order, connection, **kwargs):
    os.chdir(os.path.dirname(__file__))
    env = Environment(loader=FileSystemLoader(searchpath="./templates"))
    template = env.get_template("snowflake_task.j2")
    if "file" in kwargs and "command" not in kwargs:
        command = "./" + str(kwargs.get("file"))
    elif "command" in kwargs and "file" not in kwargs:
        command = str(kwargs.get("command"))
    elif "command" and "file" in kwargs:
        return "you may only have file or command, not both"
    else:
        return "ensure that you have either a file or command in your gener file"
    if "parameters" in kwargs:
        parameters = str(kwargs.get("parameters"))
        tasks = template.render(
            task_id=block_name,
            connection=connection,
            sql=command,
            parameters=parameters,
        )
    else:
        tasks = template.render(task_id=block_name, connection=connection, sql=command)

    return tasks
