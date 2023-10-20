import os
import sys
import time
import threading
import subprocess
import hashlib
import redis
import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
redis_host = ""
redis_password = ""
BUILD_TOKEN = ""
FILE_STORE_UPL = "http://file-store.rosalinux.ru/api/v1/upload"
FILE_STORE_API = "http://file-store.rosalinux.ru/api/v1/file_stores"
TWO_IN_THE_TWENTIETH = 2**20

class FileLogger:
    def __init__(self, file_path):
        try:
            self.file = open(file_path, "w")
        except:
            self.file = None

    def close(self):
        try:
            self.file.close()
        except:
            pass

    def log(self, message):
        if self.file:
            line = str(message)
            if line:
                self.file.write(line + "\n")
                self.file.flush()

class LiveLogger:
    LOG_DUMP_INTERVAL = 10
    LOG_SIZE_LIMIT = 100

    def __init__(self, key_name):
        self.key_name = key_name
        self.buffer = []
        self.log_mutex = threading.Lock()
        threading.Thread(target=self.dump_logs).start()

    def log(self, message):
        line = str(message)
        if line:
            with self.log_mutex:
                if len(self.buffer) > self.LOG_SIZE_LIMIT:
                    self.buffer.pop(0)
                self.buffer.append(line)

    def dump_logs(self):
        while True:
            time.sleep(self.LOG_DUMP_INTERVAL)
            with self.log_mutex:
                if self.buffer:
                    logs = "\n".join(self.buffer)
                    try:
                        r = redis.Redis(host=redis_host, password=redis_password)
                        r.setex(self.key_name, self.LOG_DUMP_INTERVAL + 5, logs)
                    except:
                        pass

class LiveInspector:
    CHECK_INTERVAL = 10

    def __init__(self, worker, time_living, container_name):
        self.worker = worker
        self.kill_at = time.time() + int(time_living)
        self.container_name = container_name
        threading.Thread(target=self.run).start()

    def run(self):
        while True:
            time.sleep(self.CHECK_INTERVAL)
            if self.kill_now():
                self.stop_build()

    def kill_now(self):
        if self.kill_at < time.time():
            return True
        status = self.status()
        if status == 'USR1':
            return True
        return False

    def status(self):
        q = 'abfworker::iso-worker-' + str(self.worker.build_id) + '::live-inspector'
        try:
            r = redis.Redis(host=redis_host, password=redis_password)
            return r.get(q)
        except:
            return None

    def stop_build(self):
        self.worker.status = 4
        runner = self.worker.runner
        subprocess.call(["docker", "stop", self.container_name])

class IsoRunner:
    def __init__(self, worker, options):
        self.worker = worker
        self.params = options['params']
        self.srcpath = options['srcpath']
        self.command = options['main_script']
        self.exit_status = None
        self.container_name = 'iso'
        arch = [x for x in self.params.split(' ') if x.startswith('ARCH=')][0][5:] if 'ARCH=' in self.params else 'default'
        arch = arch.replace('ARCH=', '')

        if arch == 'aarch64':
            self.docker_container = 'rosalab/rosa2021.1:aarch64'
        else:
            platform_type = options['platform']['type']
            platform_name = options['platform']['name']
            if platform_type == 'dnf':
                if platform_name == 'rosa2019.05':
                    self.docker_container = 'rosalab/rosa2019.05'
                else:
                    self.docker_container = 'rosalab/rosa2021.1'
            elif platform_type == 'mdv':
                self.docker_container = 'rosalab/rosa2016.1'
            elif platform_type == 'rhel':
                if platform_name == 'arsenic':
                    self.docker_container = 'fedora:rawhide'
                else:
                    self.docker_container = 'oraclelinux:9'

    def run_script(self):
        print("Run " + self.command)

        if self.worker.status != 4:
            self.prepare_script()
            exit_status = None
            final_command = [
                "docker", "run", "--name", self.container_name, "--rm", "--privileged=true",
                "--add-host", "abf-downloads.rosalinux.ru:192.168.76.41",
                "--add-host", "file-store.rosalinux.ru:192.168.76.51",
                "--device", "/dev/loop-control:/dev/loop-control",
                "-v", os.path.join(ROOT, 'iso_builder') + ":/home/vagrant/iso_builder",
                "-v", os.path.join(ROOT, 'iso_builder' + '/output') + ":/home/vagrant/results",
                "-v", os.path.join(ROOT, 'iso_builder' + '/output') + ":/home/vagrant/archives",
                self.docker_container,
                "/bin/bash", "-c", "cd /home/vagrant/iso_builder; chmod a+x " + self.command + "; " + self.params + " ./" + self.command
            ]
            print(final_command)
            process = subprocess.Popen(final_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in process.stdout:
                line = line.decode().strip()
                print(line)
                self.worker.live_logger.log(line)
                self.worker.file_logger.log(line)
            process.wait()
            self.worker.file_logger.close()
            if self.worker.status != 4:
                if exit_status is None or exit_status != 0:
                    self.worker.status = 1
                else:
                    self.worker.status = 0
            subprocess.call(["sudo", "rm", "-rf", os.path.join(ROOT, 'iso_builder')])

    def prepare_script(self):
        file_name = self.srcpath.split('archive/')[1]
        folder_name = self.srcpath.split('/')[-2]
        branch = file_name.replace('.tar.gz', '')

        command = "cd " + ROOT + "; " \
                  "curl -O -L " + self.srcpath + "; " \
                  "tar -zxf " + file_name + "; " \
                  "sudo rm -rf iso_builder; " \
                  "mv " + branch + " iso_builder; " \
                  "rm -rf " + file_name
        subprocess.call(command, shell=True)

class IsoWorker:
    def __init__(self, options):
        self.options = options
        self.build_id = options['id']
        self.status = 3
        self.runner = IsoRunner(self, options)
        self.live_logger = LiveLogger("abfworker::iso-worker-" + str(self.build_id))
        self.file_logger = FileLogger(os.path.join(ROOT, 'iso_builder' + '/output') + "/iso_build.log")
        self.live_inspector = LiveInspector(self, options['time_living'], "iso" + str(self.build_id))

    def perform(self):
        #self.runner.run_script()
        self.send_results()

    def send_results(self):
        print("send results")
        self.update_build_status_on_abf({
            'results': self.upload_results_to_file_store(),
            'exit_status': self.runner.exit_status
        })

    def update_build_status_on_abf(self, args={}):
        print('update build status on redis')
        worker_args = {
            'id': self.build_id,
            'status': self.status
        }
        print(worker_args)
        worker_args.update(args)
        try:
            r = redis.Redis(host=redis_host, password=redis_password)
            r.rpush('iso_worker_observer', worker_args)
        except:
            pass

    def upload_file_to_file_store(self, file_name):
        path_to_file = file_name
        print(BUILD_TOKEN)
        if os.path.isfile(path_to_file):
            sha1 = hashlib.sha1()
            with open(path_to_file, 'rb') as f:
                while True:
                    data = f.read(65536)
                    if not data:
                        break
                    sha1.update(data)
            sha1 = sha1.hexdigest()
            file_size = round(os.path.getsize(path_to_file) / TWO_IN_THE_TWENTIETH, 2)

            while True:
                try:
                    response = requests.get(FILE_STORE_API + ".json?hash=" + sha1)
                    if sha1 in response.text:
                        break
                    command = "curl --user " + BUILD_TOKEN + ": -POST -F \"file_store[file]=@" + path_to_file + "\" " + FILE_STORE_UPL + " --connect-timeout 5 --retry 5"
                    subprocess.call(command, shell=True)
                except:
                    pass

            subprocess.call(["sudo", "rm", "-rf", path_to_file])
            return {'sha1': sha1, 'file_name': os.path.basename(file_name), 'size': file_size}

    def upload_results_to_file_store(self):
        uploaded = []
        results_folder = os.path.join(ROOT, 'iso_builder' + '/output')
        if os.path.exists(results_folder) and os.path.isdir(results_folder):
            for root, dirs, files in os.walk(results_folder):
                for file in files:
                    print(file)
                    filename = os.path.join(root, file)
                    uploaded.append(self.upload_file_to_file_store(filename))
        return uploaded

import sys

if __name__ == "__main__":
    if "--redis" not in sys.argv or "--password" not in sys.argv or "--token" not in sys.argv:
        print("Usage: python isobuilder.py --redis <redis_host> --password <redis_password> --token <BUILD_TOKEN>")
        sys.exit(1)

    redis_host = sys.argv[sys.argv.index("--redis") + 1]
    redis_password = sys.argv[sys.argv.index("--password") + 1]
    BUILD_TOKEN = sys.argv[sys.argv.index("--token") + 1]

    options = {
        'id': 1,
        'params': 'ARCH=x86_64 PLATFORM=rosa2023.1 ABF=1 DE=SERVER',
        'srcpath': 'https://abf.io/soft/rosa-build-iso/archive/rosa-build-iso-master.tar.gz',
        'main_script': 'build-iso-abf.sh',
        'platform': {
            'type': 'dnf',
            'name': 'rosa2021.1'
        },
        'time_living': 3600
    }

    worker = IsoWorker(options)
    worker.perform()

