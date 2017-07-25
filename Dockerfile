FROM ubuntu:xenial

RUN apt-get update && apt-get install -y \
  g++-4.8 \
  gcc-4.8 \
  git \
  python2.7 python2.7-dev python-pip \
  libmysqlclient-dev libprotobuf-dev protobuf-compiler
RUN pip install -U pip \
 && pip install virtualenv

WORKDIR /app
CMD MYSQLXPB_PROTOBUF_INCLUDE_DIR=/usr/include/google/protobuf MYSQLXPB_PROTOBUF_LIB_DIR=/usr/local/lib/python3.6/site-packages/google/protobuf MYSQLXPB_PROTOC=/usr/bin/protoc python setup.py bdist_wheel --with-mysql-capi=$(which mysql_config)
