from pathlib import Path

def get_project_dir(project):
    cwd = Path.cwd()
    if cwd.name == project:
        project_dir = cwd
    else:
        for p in cwd.parents:
            if p.name == project:
                project_dir = p
                break
    return project_dir

import sys
def printse(text):
    print(">>>>", text, file=sys.stderr)