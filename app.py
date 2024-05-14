import time
import json
import os
from datetime import datetime
import subprocess
import requests


from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
SOURCE_DIR = 'input'
OUTPUT_DIR = 'output'
PROCESSED_DIR = 'processed'
ENDPOINT_URL = 'http://localhost:5080/api/default/nfdump1/_json'
BATCH_SIZE = 1000
USERNAME = 'root@example.com'
PASSWORD = 'Complexpass#123'


class NFDumpHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        # if event.src_path.endswith('.nfdump'):
        self.process_file(event.src_path)

    def process_file(self, file_path):
        output_file = os.path.join(
            OUTPUT_DIR, os.path.basename(file_path) + '.json')
        # Convert to JSON using nfdump
        subprocess.run(
            f'nfdump -r {file_path} -o json > {output_file}', shell=True)

        # Process JSON
        with open(output_file, 'r+') as f:
            data = json.load(f)
            for record in data:
                received = record.get('received')
                dt = datetime.strptime(received, '%Y-%m-%dT%H:%M:%S.%f')
                record['_timestamp'] = int(dt.timestamp())
            f.seek(0)
            json.dump(data, f)
            f.truncate()

        # Send data in batches
        print("Sending data for file ", output_file)
        self.send_data_in_batches(data, output_file)

        # Move the original file to processed
        os.rename(file_path, os.path.join(
            PROCESSED_DIR, os.path.basename(file_path)))

    def send_data_in_batches(self, data, output_file):
        headers = {'Content-Type': 'application/json'}
        auth = (USERNAME, PASSWORD)

        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]
            try:
                response = requests.post(
                    ENDPOINT_URL, json=batch, auth=auth, headers=headers, timeout=10
                )
                if response.status_code == 200:
                    print(
                        f'{datetime.now()} Successfully sent {len(batch)} records')
                else:
                    print(
                        f'Failed to send data for {output_file}: {response.text}')
                    break
            except requests.exceptions.RequestException as e:
                print(f'Error occurred: {e}')
                break


if __name__ == '__main__':
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
    event_handler = NFDumpHandler()
    observer = Observer()
    observer.schedule(event_handler, SOURCE_DIR, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
            print(f'{datetime.now()} Watching for new files...')
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
