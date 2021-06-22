FROM python:3.8-slim-buster
RUN pip install redis 
RUN apt update && apt install telnet
ADD ./test_script.py /
CMD python /test_script.py