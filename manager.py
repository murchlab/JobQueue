# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 14:04:16 2022

@author: Xingrui Song
"""
from packaging import version
from platform import python_version

if version.parse(python_version()) > version.parse('3.8'):
    from multiprocessing.shared_memory import SharedMemory
else:
    from shared_memory import SharedMemory

from collections import deque
import pickle
import time
from threading import Thread
from datetime import datetime
import sys
import inspect


def running_in_Spyder():
    result = False
    for frame in inspect.stack():
        if 'spyder' in str(frame):
            result = True
            break
    return result


if running_in_Spyder():
    from tqdm import tqdm
else:
    from tqdm.auto import tqdm

    
if sys.platform == 'win32':
    timestamp_format = '%b %#d, %#I:%M:%S %p'
elif sys.platform == 'linux':
    timestamp_format = '%b %-d, %-I:%M:%S %p'
else:
    timestamp_format = '%b %d, %I:%M:%S %p'


lock_interval = 0.05  # seconds
lock_timeout = 10  # seconds
jobqueue_interval = 0.05  # seconds
jobqueue_timeout = 10  # seconds
heartbeat_interval = 0.05  # seconds


class ShmBool:
    size = 1
    
    def __init__(self, shm, buf_index):
        self.shm = shm
        self.buf_index = buf_index
        
    def read(self):
        return bool(self.shm.buf[self.buf_index])
    
    def write(self, value):
        self.shm.buf[self.buf_index] = value
        
        
class ShmInt:
    size = 8
    
    def __init__(self, shm, buf_index):
        self.shm = shm
        self.buf_index = buf_index
        
    def read(self):
        return int.from_bytes(self.shm.buf[self.buf_index:self.buf_index + 8], byteorder='little')
    
    def write(self, value):
        self.shm.buf[self.buf_index:self.buf_index + 8] = value.to_bytes(8, byteorder='little')
        
        
class ShmLock:
    size = 1
    
    def __init__(self, shm, buf_index):
        self.lock = ShmBool(shm, buf_index)
        
    def acquire(self):
        t = 0
        acquire_success = False
        while t < lock_timeout:
            if self.lock.read():
                time.sleep(lock_interval)
                t += lock_interval
            else:
                self.lock.write(True)
                acquire_success = True
                break
            
        if not acquire_success:
            print('Warning: Timeout. Force acquiring the spinlock.')
            self.lock.write(True)
            
        return acquire_success
            
    def release(self):
        self.lock.write(False)
        
        
class ShmLockManager:
    def __init__(self, lock):
        self.lock = lock
        self.acquire_success = lock.acquire()
        
    def __enter__(self):
        pass
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        self.lock.release()
        
        
class ShmRWLock:
    size = ShmInt.size + 2 * ShmLock.size  # 10 bytes
    
    def __init__(self, shm, buf_index):
        self.num_readers = ShmInt(shm, buf_index)
        buf_index += ShmInt.size
        self.lock_num_readers = ShmLock(shm, buf_index)
        buf_index += ShmLock.size
        self.lock_writer = ShmLock(shm, buf_index)
        
    def _lock_num_readers(self):
        return ShmLockManager(self.lock_num_readers)
        
    def reset(self):
        self.num_readers.write(0)
        self.lock_num_readers.release()
        self.lock_writer.release()
        
    def acquire_r(self):
        with self._lock_num_readers():
            num_readers = self.num_readers.read()
            if not num_readers:
                self.lock_writer.acquire()
            self.num_readers.write(num_readers + 1)
        
    def release_r(self):
        with self._lock_num_readers():
            num_readers = max(self.num_readers.read() - 1, 0)
            if not num_readers:
                self.lock_writer.release()
            self.num_readers.write(num_readers)
        
    def acquire_w(self):
        if not self.lock_writer.acquire():
            self.num_readers.write(0)
            self.lock_num_readers.release()
        
    def release_w(self):
        self.lock_writer.release()
        
        
class ShmObject:
    def __init__(self, shm, buf_index):
        self.shm = shm
        self.size = ShmInt(shm, buf_index)
        self.pickle_index = buf_index + 8
        
    def read(self):
        pickle_bin = self.shm.buf[self.pickle_index:self.pickle_index + self.size.read()]
        return pickle.loads(pickle_bin)
    
    def write(self, obj):
        pickle_bin = pickle.dumps(obj)
        size = len(pickle_bin)
        self.size.write(size)
        self.shm.buf[self.pickle_index:self.pickle_index + size] = pickle_bin
        
        
class ShmLockedObject:
    def __init__(self, shm, buf_index):
        self.lock = ShmRWLock(shm, buf_index)
        buf_index += ShmRWLock.size
        self.object = ShmObject(shm, buf_index)
        
    def read(self):
        obj = self.object.read()
        return obj
        
    def write(self, obj):
        self.object.write(obj)
        
    def acquire_r(self):
        self.lock.acquire_r()
        
    def release_r(self):
        self.lock.release_r()
        
    def acquire_w(self):
        self.lock.acquire_w()
        
    def release_w(self):
        self.lock.release_w()
        
    def reset_lock(self):
        self.lock.reset()
    
    
class OpenShmLockedObject:
    def __init__(self, locked_object, flag):
        self.object = locked_object
        self.flag = flag
        self.object_local = None
        if flag == 'r':
            locked_object.acquire_r()
        elif flag == 'w':
            locked_object.acquire_w()
        else:
            raise Exception(f"Unknown flag '{flag}'")
        try:
            self.object_local = self.object.read()
        except:
            self.object_local = {}
            
    def __enter__(self):
        return self.object_local

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.flag == 'r':
            self.object.release_r()
        else:  # w
            self.object.write(self.object_local)
            self.object.release_w()
    
    
class JobQueue:
    def __init__(self, user='', message='', total=None, reset=False, enter=True):
        self.user = user
        self.message = message
        self.total = total
        self.enter = enter
        self.job_id = 0
        self.last_modified = time.time()
        self.last_heatbeat = time.time()
        self.last_progress = time.time()
        self.current_job_id = 0
        self.done_waiting = False
        self.heartbeat_on = False
        self.pbar=None
        self.pbar_last_total = 0
        
        is_new_shm = False
        try:
            self.shm = SharedMemory('jobqueue')
        except:
            self.shm = SharedMemory('jobqueue', create=True, size=2 ** 20)
            is_new_shm = True
        
        self.jobqueue = ShmLockedObject(self.shm, 0)
        if is_new_shm or reset:
            self.init()
        
        if enter:
            self._enter()
            
    def open_jobqueue(self, flag):
        return OpenShmLockedObject(self.jobqueue, flag)
        
    def init(self):
        self.jobqueue.reset_lock()
        with self.open_jobqueue('w') as jobqueue:
            jobqueue.clear()
            jobqueue.update({
                'job_id_counter': 0,
                'last_modified': time.time(),
                'last_heartbeat': time.time(),
                'last_progress': time.time(),
                'execution_time': time.time(),
                'finished': 0,
                'jobqueue': deque()
            })
            
    def _heartbeat(self):
        self.heartbeat_on = True
        while self.heartbeat_on:
            try:
                with self.open_jobqueue('w') as jobqueue:
                    jobqueue['last_heartbeat'] = time.time()
                    self.display_pbar(jobqueue, leave=True)
            except:
                pass
            time.sleep(heartbeat_interval)
        
    def start_heartbeat(self):
        t = Thread(target=self._heartbeat)
        t.start()
        
    def set_finished(self, n):
        with self.open_jobqueue('w') as jobqueue:
            jobqueue['finished'] = n
            self.last_progress = time.time()
            jobqueue['last_progress'] = self.last_progress
            
    def set_total(self, total):
        with self.open_jobqueue('w') as jobqueue:
            jobqueue['jobqueue'][0]['total'] = total
            self.last_progress = time.time()
            jobqueue['last_progress'] = self.last_progress
        
    def update(self, n=1):
        with self.open_jobqueue('w') as jobqueue:
            jobqueue['finished'] += n
            self.last_progress = time.time()
            jobqueue['last_progress'] = self.last_progress
        
    def display_jobqueue(self, jobqueue):
        try:
            offsets = [8, 20, 44]
            def string_tab(strings, offsets):
                offsets = offsets.copy()
                offsets.append(0)
                s = ''
                for string, offset in zip(strings, offsets):
                    s += string
                    if len(s) < offset:
                        s += ' ' * (offset - len(s))
                    else:
                        s += ' '
                return s

            def display_titles():
                s = string_tab(['Job ID', 'User', 'Creation time', 'Message'], offsets)
                print(s)

            def display_job(job):
                string_list = [
                    str(job['job_id']),
                    job['user'],
                    datetime.fromtimestamp(job['creation_time']).strftime(timestamp_format),
                    job['message']
                ]
                s = string_tab(string_list, offsets)
                print(s)

            print()
            if len(jobqueue['jobqueue']) > 1:
                print('--------------------Waiting jobs-------------------')
                display_titles()

                for job in list(jobqueue['jobqueue'])[1:]:
                    display_job(job)

                print()
                print()

            if len(jobqueue['jobqueue']):
                current_job = jobqueue['jobqueue'][0]
                print('--------------------Current job--------------------')
                display_titles()
                display_job(current_job)
                print()

                print('Execution time')
                print(datetime.fromtimestamp(jobqueue['execution_time']).strftime(timestamp_format))
                
            else:
                print('No job is running.')
            
        except Exception as e:
            print(e)
            
    def display_pbar(self, jobqueue, leave=False):
        current_job = jobqueue['jobqueue'][0]
        if current_job['total'] is not None:
            if current_job['total'] != self.pbar_last_total:
                self.pbar_last_total = current_job['total']
                if self.pbar is not None:
                    self.pbar.close()
                    self.pbar = None
                
            if self.pbar is None:
                self.pbar = tqdm(total=current_job['total'], bar_format="{l_bar}{bar} [time left: {remaining}]", leave=leave)
                
            if (self.pbar.last_print_n == 0) and jobqueue['finished'] :
                self.pbar.last_print_t = self.pbar.start_t = self.pbar._time()
            self.pbar.last_print_n = self.pbar.n = jobqueue['finished']
            self.pbar.refresh()
            
    def close_pbar(self):
        if self.pbar is not None:
            self.pbar.n = self.pbar.total
            self.pbar.last_print_n = self.pbar.total
            self.pbar.update(0)
            self.pbar.refresh()
            self.pbar.close()
            self.pbar = None
        
    def _enter(self):
        try:
            with self.open_jobqueue('w') as jobqueue:
                self.job_id = jobqueue['job_id_counter'] + 1
                jobqueue['job_id_counter'] = self.job_id
                jobqueue['jobqueue'].append({
                    'job_id': self.job_id,
                    'user': self.user,
                    'creation_time': time.time(),
                    'message': self.message,
                    'total': self.total
                })
                self.last_modified = time.time()
                jobqueue['last_modified'] = self.last_modified
            first_loop = True
            self.done_waiting = False
            while True:
                with self.open_jobqueue('r') as jobqueue:
                    self.current_job_id = jobqueue['jobqueue'][0]['job_id']
                    if self.current_job_id == self.job_id:
                        break
                    self.last_heartbeat = jobqueue['last_heartbeat']
                    if first_loop:
                        print(f'Waiting for other jobs to finish... Your job ID is {self.job_id}.')
                        print()
                        self.display_jobqueue(jobqueue)
                        first_loop = False
                    elif jobqueue['last_modified'] != self.last_modified:
                        self.close_pbar()
                        self.display_jobqueue(jobqueue)
                    self.display_pbar(jobqueue)
                    self.last_modified = jobqueue['last_modified']
                    self.last_progress = jobqueue['last_progress']
                if time.time() - self.last_heartbeat > jobqueue_timeout:
                    self.close_pbar()
                    print(f'Job {self.current_job_id} is not responding. Cleaning up...')
                    with self.open_jobqueue('w') as jobqueue:
                        jobqueue['jobqueue'].popleft()
                        self.last_modified = time.time()
                        jobqueue['last_modified'] = self.last_modified
                    break
                time.sleep(jobqueue_interval)
            self.done_waiting = True
        except KeyboardInterrupt:
            self.__exit__(KeyboardInterrupt, None, None)
            print('Exited the JobQueue.')
            raise KeyboardInterrupt
        if self.done_waiting:
            self.close_pbar()
            print('Done waiting.')
            self.start_heartbeat()
            with self.open_jobqueue('w') as jobqueue:
                jobqueue['execution_time'] = time.time()
                jobqueue['finished'] = 0
                self.last_modified = time.time()
                jobqueue['last_modified'] = self.last_modified
            
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        unknown_exception = False
        if self.pbar is not None:
            self.close_pbar()
        if self.enter:
            with self.open_jobqueue('w') as jobqueue:
                for i, job in enumerate(jobqueue['jobqueue']):
                    if job['job_id'] == self.job_id:
                        if i == 0:
                            jobqueue['jobqueue'].popleft()
                        elif i == len(jobqueue['jobqueue']) - 1:
                            jobqueue['jobqueue'].pop()
                        else:
                            del jobqueue['jobqueue'][i]
                        break
#                 self.last_modified = time.time()
#                 jobqueue['last_modified'] = self.last_modified
            if self.done_waiting:
                if exc_type is None:
                    print('Job finished.')
                elif exc_type == KeyboardInterrupt:
                    print('Job interrupted.')
                else:
                    unknown_exception = True
            self.heartbeat_on = False
        self.shm.close()
        if not unknown_exception:
            return True
        
                
            
    
