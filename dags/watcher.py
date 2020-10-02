import os
import subprocess

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer


def trigger_dag(filename):
    data = '{"process_file_name":"' + filename + '" }'
    process = subprocess.Popen(['airflow', 'trigger_dag', 'process_log_file', '--conf', data],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    print(stdout)
    print(stderr)
    return stdout, stderr


class Handler(FileSystemEventHandler):

    def on_created(self, event: FileSystemEvent):
        if event.event_type == 'created':
            print("file created", event.src_path)
            if event.src_path.endswith('.log'):
                print('Executing the dag to process log file')
                trigger_dag(event.src_path)
            else:
                print('File is not a log file will not trigger dag')


def main():
    observer = Observer()
    event_handler = Handler()
    observer_path = os.path.join(os.path.dirname(__file__), 'apache_logs')
    observer.schedule(event_handler, observer_path, recursive=False)
    observer.start()
    return observer


if __name__ == '__main__':
    import time

    observer = main()
    try:
        while True:
            time.sleep(2)
    except Exception as ex:
        observer.stop()
