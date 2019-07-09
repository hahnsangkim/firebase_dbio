# IO Helper for PostgreSQL and Firebase

## Prerequisite 
* PostgreSQL is up and running
* Firebase API is set up
* Populate .env.sample and then rename it as .env

## Installation
```bash
$ virtualenv --python=python3 envs
$ source envs/bin/activate
(envs) $ pip install psycopg2-binary 
(envs) $ pip install pyrebase
(envs) $ pip install sqlalchemy
```


## Description
* Data: Time-series
* Format: pandas and json
* Data format to and from Firebase: JSON

