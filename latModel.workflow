workflow "latModel analysis" {
  #on = "push"
  resolves = "run notebook"
}

action "install python modules" {
  uses = "jefftriplett/python-actions@master"
  args = "pip install -r /github/workspace/scripts/requirements.txt"
}

action "run notebook" {
  needs = ["install python modules"]
  uses = "jefftriplett/python-actions@master"
  args = [
    "python /github/workspace/scripts/run-nb-experiment.py",
    "latModel.ipynb",
    "latModel-out.ipynb",
    "parameters.json"
  ]
}
