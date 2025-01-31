from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime
import time

# Kết nối tới Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}], retry_on_timeout=True)
if not es.ping():
    raise ValueError("Kết nối tới Elasticsearch không thành công!")

# Kết nối tới Kafka cluster
consumer = KafkaConsumer(
    'test_topic',  # Tên topic Kafka
    bootstrap_servers=['10.7.4.109:9092', '10.7.4.109:9093', '10.7.4.109:9094'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # Tắt auto commit
    group_id='my-group',  # Group ID cho Kafka consumer
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Chuyển dữ liệu từ JSON về object
)

def compute_doc_id(doc):
    """Dựa trên các trường F_MAMH, F_MASV, F_KHOAHOC, và NHHK."""
    if all(key in doc for key in ['F_MAMH', 'F_MASV', 'F_KHOAHOC', 'NHHK']):
        return f"{doc['F_MAMH']}_{doc['F_MASV']}_{doc['F_KHOAHOC']}_{doc['NHHK']}"
    elif 'F_MAMH' in doc and 'F_MASV' in doc and 'F_KHOAHOC' in doc:
        return f"{doc['F_MAMH']}_{doc['F_MASV']}_{doc['F_KHOAHOC']}"
    else:
        return None

def process_and_index(doc, timestamp):
    try:
        # Tính timestamp mới từ giá trị trong message
        timestamp_ms = timestamp / 1000.0  # Chuyển timestamp từ Kafka (milisecond) sang giây
        doc['@timestamp'] = datetime.utcfromtimestamp(timestamp_ms).isoformat()

        # Kiểm tra và chuyển đổi giá trị trong F_DIEM2
        if 'F_DIEM2' in doc:
            try:
                doc['F_DIEM2'] = float(doc['F_DIEM2'])
            except (ValueError, TypeError):
                print(f"Cảnh báo: Giá trị F_DIEM2 không hợp lệ: {doc['F_DIEM2']}")

        # Thêm trường mới dựa trên giá trị của F_TENLOP
        if 'F_TENLOP' in doc and 'F_TENMHVN' in doc:
            if doc['F_TENLOP'].startswith('FL'):
                doc['NNA'] = doc['F_TENMHVN']
                doc['DIEM_NNA'] = doc.get('F_DIEM2', None)
            elif doc['F_TENLOP'].startswith('DI'):
                doc['MMT'] = doc['F_TENMHVN']
                doc['DIEM_MMT'] = doc.get('F_DIEM2', None)

        # Tính toán ID duy nhất cho tài liệu
        doc_id = compute_doc_id(doc)

        if doc_id is not None:
            index_name = f"phantich1-{datetime.now().strftime('%Y.%m.%d')}"
            es.index(
                index=index_name,
                document=doc,
                id=doc_id,  # ID duy nhất cho tài liệu
            )
            print(f"Đã gửi tài liệu tới Elasticsearch: {doc}")
        else:
            print(f"Không thể tạo ID cho tài liệu: {doc}")

    except Exception as e:
        print(f"Lỗi khi gửi tài liệu tới Elasticsearch: {e}")

def consume_messages():
    batch_size = 100
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for message in consumer:
            doc = message.value
            futures.append(executor.submit(process_and_index, doc, message.timestamp))

            if len(futures) >= batch_size:
                for future in futures:
                    future.result()
                consumer.commit()
                futures.clear()

        if futures:
            for future in futures:
                future.result()
            consumer.commit()

try:
    consume_messages()
finally:
    consumer.close()

