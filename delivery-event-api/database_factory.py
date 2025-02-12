import csv
import redis
from typing import Any, Dict


class BaseDatabase:
    def get(self, key: str) -> Any:
        raise NotImplementedError

    def size(self):
        raise NotImplementedError

class CSVDatabase(BaseDatabase):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = self._load_csv()

    def _load_csv(self) -> Dict[str, Dict]:
        """ CSV 파일을 읽어 Dictionary 형태로 저장 """
        data = {}
        with open(self.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for idx, row in enumerate(reader):
                key = idx+1
                data[f"addr:{key}"] = row
        return data
    def get(self, key: str) -> Any:
        return self.data.get(key, None)

    def size(self) -> int:
        return len(self.data)



class RedisDatabase(BaseDatabase):
    def __init__(self, host="localhost", port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def get(self, key: str) -> Any:
        hget_data = self.client.hgetall(key)

        return {key.decode('utf-8'): value.decode('utf-8') for key, value in hget_data.items()}

    def size(self) -> int:
        return self.client.dbsize()


class DatabaseFactory:
    @staticmethod
    def get_database(db_type: str, **kwargs) -> BaseDatabase:
        if db_type == "csv":
            return CSVDatabase(kwargs["file_path"])
        elif db_type == "redis":
            return RedisDatabase(host=kwargs.get("host", "localhost"), port=kwargs.get("port", 6379))
        else:
            raise ValueError(f"지원하지 않는 데이터베이스 유형: {db_type}")


# if __name__ == "__main__":
#     csv_db = DatabaseFactory.get_database("csv", file_path="addr_data.csv")
#     print(csv_db.get("addr:1"))  # ID가 1인 데이터 가져오기

    # Redis로 데이터 가져오기
    # redis_db = DatabaseFactory.get_database("redis", host="localhost", port=6379)
    # print(redis_db.get("user:1"))  # 데이터 조회
