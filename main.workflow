
workflow "rados simulation" {
  #on = "push"
  resolves = "run simulation"
}

action "install python modules" {
  uses = "jefftriplett/python-actions@master"
  args = "pip install -r requirements.txt"
}

action "run simulation" {
  needs = ["install python modules"]
  uses = "jefftriplett/python-actions@master"
  args = [
    "workflows/minimal-python/scripts/radossim.py"
  ]
}
