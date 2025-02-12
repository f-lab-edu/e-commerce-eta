import random
import argparse
import time
import json
import os
from datetime import datetime, timezone
from database_factory import DatabaseFactory
from message_sender import MessageSenderFactory  # 팩토리 패턴 적용

# 전역 변수 설정
CURRENT_DIR = os.path.dirname(__file__)
DB_FILE_PATH = os.path.join(CURRENT_DIR, 'files/addr_data.csv')
HUB_FILE_PATH = os.path.join(CURRENT_DIR, 'files/hub_terminal.json')
SUB_FILE_PATH = os.path.join(CURRENT_DIR, 'files/sub_terminal.json')

def load_json(file_path):
    """JSON 파일을 로드하는 함수"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_args():
    """커맨드라인 인자 파싱"""
    parser = argparse.ArgumentParser(description='Create Delivery Request Event Data')
    parser.add_argument('-c', '--count', type=int, metavar='', required=True, help='Counts of Event')
    return parser.parse_args()

def generate_event(db_instance, hub, sub):
    """이벤트 데이터 생성 함수"""
    offset_datetime = datetime.now(timezone.utc)

    random_num = random.randint(1, db_instance.size())  # 배송 목적지 생성
    hub_num = random.randint(1, 3)  # HUB 터미널 정보

    addr_key = f"addr:{random_num}"
    addr_value = db_instance.get(addr_key)

    return {
        'event_id': f'EVT-{offset_datetime.strftime("%Y%m%d%H%M%S")}-{random.randint(1000, 9999)}',
        'type': '/delivery_request',
        'datetime': offset_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        'hub_station': hub.get(f'hub:{hub_num}', 'Unknown HUB'),
        'sub_station': sub.get(addr_value.get('district_name', ''), 'Unknown Sub'),
        'destination': {
            'province_name': addr_value.get('province_name', ''),
            'district_name': addr_value.get('district_name', ''),
            'township': addr_value.get('township', ''),
            'road_name': addr_value.get('road_name', ''),
            'full_address': addr_value.get('full_address', ''),
            'latitude': float(addr_value.get('latitude', 0.0)),
            'longitude': float(addr_value.get('longitude', 0.0))
        }
    }

def main():
    """메인 실행 함수"""
    args = get_args()
    db_instance = DatabaseFactory.get_database("csv", file_path=DB_FILE_PATH)

    # JSON 데이터 로드
    hub = load_json(HUB_FILE_PATH)
    sub = load_json(SUB_FILE_PATH)

    # 메시지 전송기 선택 (팩토리 패턴 적용)
    sender = MessageSenderFactory.get_sender(
        "rabbitmq",
        host="localhost",
        queue_name="delivery_events",
        user="dawn",
        password="dawn"
    )

    for _ in range(args.count):
        event_data = generate_event(db_instance, hub, sub)
        sender.send(event_data)  # 메시지 전송
        with open('./test.json', 'w', encoding='utf-8') as w:
            json.dump(event_data, w, ensure_ascii=False, indent=4)
        time.sleep(random.uniform(0.5, 1.5))  # 랜덤한 딜레이 적용

if __name__ == '__main__':
    main()
