FROM python:3
ADD test_script.py /
RUN pip install redis 
RUN pip install pyprobables
EXPOSE 8080
CMD [ "python", "./test_script.py" ]
