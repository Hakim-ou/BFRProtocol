FROM python:3.8-slim-buster
ADD ./test_script.py /
ADD ./script.sh /
RUN pip install redis 
RUN pip install pyprobables
RUN apt update && apt install telnet
EXPOSE 8080
CMD bash /script.sh
