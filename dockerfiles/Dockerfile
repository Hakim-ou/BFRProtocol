FROM python:3
ADD test_script.py /
RUN pip install redis 
RUN pip install pyprobables
CMD [ "python", "./test_script.py" ]
