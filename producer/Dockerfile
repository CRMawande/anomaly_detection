# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory to /producer
WORKDIR /producer

# Copy the current directory contents into the container at /producer
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run producer.py when the container launches
CMD ["python", "producer.py"]
