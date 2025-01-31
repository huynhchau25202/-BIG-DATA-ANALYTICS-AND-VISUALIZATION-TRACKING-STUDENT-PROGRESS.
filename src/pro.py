import hashlib
import json
import openpyxl
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewPartitions
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
from datetime import datetime

# Kafka configuration
KAFKA_BROKERS = ['10.7.4.109:9092', '10.7.4.109:9093', '10.7.4.109:9094']
KAFKA_TOPIC = 'test_topic'

# Folder to watch
FOLDER_TO_WATCH = '/home/huynhchau/Desktop/read_excel'
file_hash_data = {}

# Kafka producer initialization
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Biến toàn cục để quản lý timestamp
last_timestamp = time.time()

def compute_row_hash(row):
    """Generate a hash for a row of data."""
    row_string = json.dumps(row, sort_keys=True)
    return hashlib.md5(row_string.encode()).hexdigest()

def read_excel_to_json(file_path):
    """Read an Excel file and convert it to a list of JSON objects."""
    workbook = openpyxl.load_workbook(file_path)
    sheet = workbook.active
    headers = [cell.value for cell in sheet[1]]
    json_data = []
    for row in sheet.iter_rows(min_row=2, values_only=True):
        if any(row):
            row_dict = {headers[i]: (row[i] if row[i] is not None else '') for i in range(len(headers))}
            json_data.append(row_dict)
    return json_data, headers

def get_differences(new_data, old_hashes):
    """Compare new data to old hashes to find differences."""
    differences = []
    new_hashes = []
    for row in new_data:
        row_hash = compute_row_hash(row)
        new_hashes.append(row_hash)
        if row_hash not in old_hashes:
            differences.append(row)
    return differences, new_hashes

def process_excel_file(file_path):
    """Process an Excel file and send its data to Kafka."""
    global last_timestamp  # Sử dụng biến toàn cục
    try:
        new_data, headers = read_excel_to_json(file_path)

        if file_path in file_hash_data:
            old_hashes = file_hash_data[file_path]
            data_to_send, new_hashes = get_differences(new_data, old_hashes)
        else:
            data_to_send = new_data
            new_hashes = [compute_row_hash(row) for row in new_data]

        for json_obj in data_to_send:
            # Tăng timestamp thêm 0.0001 giây mỗi lần gửi
            last_timestamp += 0.0001
            json_obj['@timestamp'] = datetime.fromtimestamp(last_timestamp).isoformat(timespec='microseconds') + 'Z'

            # Loại bỏ các giá trị None hoặc rỗng
            cleaned_json_obj = {k: v for k, v in json_obj.items() if v not in (None, '')}

            # Đảm bảo @timestamp là khóa đầu tiên trong JSON
            reordered_json_obj = {'@timestamp': json_obj['@timestamp']}
            for key, value in cleaned_json_obj.items():
                if key != '@timestamp':  # Tránh trùng lặp @timestamp
                    reordered_json_obj[key] = value

            print(f"Sending to Kafka: {json.dumps(reordered_json_obj, indent=2, ensure_ascii=False)}")
            producer.send(KAFKA_TOPIC, value=reordered_json_obj)

        # Lưu các hash mới vào bộ nhớ
        file_hash_data[file_path] = new_hashes
        producer.flush()
        print(f"Completed sending data from {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

class ExcelFileHandler(FileSystemEventHandler):
    """Handler to monitor file system events for Excel files."""
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.xlsx') or event.src_path.endswith('.xls'):
            print(f"New Excel file detected: {event.src_path}")
            process_excel_file(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.xlsx') or event.src_path.endswith('.xls'):
            print(f"Excel file modified: {event.src_path}")
            process_excel_file(event.src_path)

def start_observer():
    """Start the directory observer."""
    event_handler = ExcelFileHandler()
    observer = Observer()
    observer.schedule(event_handler, FOLDER_TO_WATCH, recursive=False)
    observer.start()
    try:
        print(f"Monitoring folder: {FOLDER_TO_WATCH}")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    # Start monitoring folder for Excel file changes
    observer_thread = threading.Thread(target=start_observer)
    observer_thread.start()

