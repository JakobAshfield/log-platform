#base image
FROM python:3.11-slim
#set working directory
WORKDIR /app
#copy requirements file
COPY requirements.txt . 
RUN pip install -r requirements.txt
#copy app code
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]