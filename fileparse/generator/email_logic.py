import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from yaml import loader


def email_logic(block_name, email_to, subject, text, order, **kwargs):
    os.chdir(os.path.dirname(__file__))
    env = Environment(loader=FileSystemLoader(searchpath="./templates"))
    template = env.get_template("email_task.j2")
    html_content = """<h3>{}</h3>""".format(text)

    tasks = template.render(
        task_id=block_name,
        email_to=email_to,
        subject=subject,
        html_content=html_content,
    )
    return tasks
