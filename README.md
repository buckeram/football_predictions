# Football Predictions

A serverless application to make predictions about the outcomes of upcoming
football (soccer) matches. Runs on AWS.

## Getting started

Execute these commands:

```
$ python3 -m venv .venv
$ source .venv/bin/activate
$ python3 -m pip install --upgrade pip
$ pip install chalice
$ chalice new-project football_predictions
$ cd football_predictions
$ pip install aws_lambda_powertools requests
$ pip freeze | grep aws_lambda_powertools >> requirements.txt
$ pip freeze | grep requests >> requirements.txt
```
