
workflow "rados simulation" {
  #on = "push"
  resolves = "run simulation"
}

action "install python modules" {
  uses = "jefftriplett/python-actions@master"
  args = "pip install -r /github/workspace/scripts/requirements.txt"
}

action "run simulation" {
  needs = ["install python modules"]
  uses = "jefftriplett/python-actions@master"
  args = [
    "python /github/workspace/scripts/radossim.py"
  ]
}
