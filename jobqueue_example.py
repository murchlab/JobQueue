import sys
sys.path.append("C:/Users/crow108/Documents/analyzer")
import paths
paths.paths()
import time


from jobqueue import JobQueue


# Replace 'example' with your name
with JobQueue('example', message='CR-Gate'):
    # Your job goes here
    for i in range(10):
        time.sleep(1)


# This usage allows other users to see your pbar
with JobQueue('example', message='msg', total=10) as jq:
    # Your job goes here
    for i in range(10):
        time.sleep(1)
        jq.update()
        