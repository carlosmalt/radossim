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
    "/github/workspace/experiments/latModel/latModel.ipynb",
    "/github/workspace/experiments/latModel/latModel-out.ipynb",
    "/github/workspace/experiments/latModel/parameters.json"
  ]
}
