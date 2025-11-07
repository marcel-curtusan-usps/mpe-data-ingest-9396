# Use official Python image
FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Set working directory
WORKDIR /home/site/wwwroot

# Copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY . .

# Expose port for Azure Functions host
EXPOSE 80

# Start Azure Functions host
CMD ["/azure-functions-host/Microsoft.Azure.WebJobs.Script.WebHost"]
