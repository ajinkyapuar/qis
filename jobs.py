from __future__ import absolute_import
from azure.storage.blob import BlockBlobService
import os
import cv2
from celery import Celery
import time


from dotenv import load_dotenv
load_dotenv()

RMQ_USER=os.environ.get('RMQ_USER')
RMQ_PWD=os.environ.get('RMQ_PWD')
RMQ_SERVER_IP=os.environ.get('RMQ_SERVER_IP')
RMQ_VHOST=os.environ.get('RMQ_VHOST')


AZ_JOB_ACCOUNT_NAME=os.environ.get('AZ_JOB_ACCOUNT_NAME')
AZ_JOB_ACCOUNT_KEY=os.environ.get('AZ_JOB_ACCOUNT_KEY')

# AZ_JOB_OP_ACCOUNT_NAME=os.environ.get('AZ_JOB_OP_ACCOUNT_NAME')
# AZ_JOB_OP_ACCOUNT_KEY=os.environ.get('AZ_JOB_OP_ACCOUNT_KEY')

app = Celery("jobs",
             broker="amqp://" + RMQ_USER + ":" + RMQ_PWD + "@" + RMQ_SERVER_IP + "/" + RMQ_VHOST,
             backend='rpc://')

block_blob_service = BlockBlobService(
    account_name=AZ_JOB_ACCOUNT_NAME,
    account_key=AZ_JOB_ACCOUNT_KEY)


@app.task
def read_blob_storage(containerName):
    print("Reading Container...")
    job_file_paths = []
    local_path = "./data/" + containerName
    generator = block_blob_service.list_blobs(containerName)
    if not os.path.exists(local_path):
        os.mkdir(local_path)
    for blob in generator:
        # print("Blob name: " + blob.name)
        # Download the blob(s).
        full_path_to_file = os.path.join(local_path, blob.name)
        # print("Downloading blob to " + full_path_to_file)
        block_blob_service.get_blob_to_path(containerName, blob.name, full_path_to_file)
        job_file_paths.append(full_path_to_file)

        to_gray.apply_async(args=[full_path_to_file])
        to_hsv.apply_async(args=[full_path_to_file])
    # return job_file_paths

@app.task
def to_gray(path):
    # print("TO GRAY...")
    image = cv2.imread(path)
    filename = os.path.basename(path)
    container = os.path.split(os.path.split(path)[-2])[-1]
    write_path = "./gray/" + container
    if not os.path.exists(write_path):
        os.mkdir(write_path)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    cv2.imwrite(write_path + "/" + filename, gray)
    return


@app.task
def to_hsv(path):
    # print("TO HSV...")
    image = cv2.imread(path)
    filename = os.path.basename(path)
    container = os.path.split(os.path.split(path)[-2])[-1]
    write_path = "./hsv/" + container
    if not os.path.exists(write_path):
        os.mkdir(write_path)
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    cv2.imwrite(write_path + "/" + filename, hsv)
    return