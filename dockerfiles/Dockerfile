FROM python:3.8-slim-buster
ADD test_script.py /
RUN pip install redis 
RUN pip install pyprobables
RUN apt update && apt install telnet
EXPOSE 8080
CMD [ "python", "./test_script.py" ]
