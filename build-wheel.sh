#!/bin/bash
docker build -t asaha/mysql-connector-python-builder .
docker run -v `pwd`:/app asaha/mysql-connector-python-builder
