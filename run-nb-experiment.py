import argparse
import json
import papermill as pm

parser = argparse.ArgumentParser()
parser.add_argument("input", help="input Jupyter notebook")
parser.add_argument("output", help="output Jupyter notebook")
parser.add_argument("parameters", help="parameter file in JSON")
args = parser.parse_args()
parameters = json.load(open(args.parameters))

pm.execute_notebook(args.input, args.output, parameters=dict(parameters))
