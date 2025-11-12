from airflow.sdk.bases.hook import BaseHook
import pandas as pd
from airflow.models import BaseOperator

class SeoulApiToCsvOperator(BaseOperator):
    """
    **kwarge에 기존 jinja 템플릿 파라미터 + template_fild 파라미터를 추가시킴
    """
    template_filed = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        """
        데이터 정의 단계
        1. url 주소 하드코딩 : url:8080/apikey/type/dataset_nm/start/end 이 self.endpoint로 불러와짐
        2. url 주소 + 데이터셋 이름(dataset_nm)결합 
        """
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        """
        데이터 실행 단계
        1. 앞의 __init__ 값 불러옴
        2. 맨 처음 1행부터 1000행까지 RestAPI 형태로 값을 불러와 데이터프레임형태로 row_df에 저장
           (_call_api 함수이용)
        3. 불러온 값의 수가 1000 이하일때까지 불러오는걸 반복(while)
        4. while 끝난 후 해당위치에 path 이름의 디렉토리가 없다면 만들어주고, 있다면 csv로 로드
        """
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        
        while True:
            self.log.info(f'시작: {start_row}')
            self.log.info(f'끝: {end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000
        
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

        def _call_api(self, base_url, start_row, end_row):
            import requests
            import json

            headers = {'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'}

            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'

            if self.base_dt is not None:
                request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
            response = requests.get(request_url, headers) # string 형태로 response에 저장됨
            contents = json.loads(response.text) # 딕셔너리 형태로 contents 로 로드

            key_nm = list(contents.key())[0]
            row_data = contents.get(key_nm).get('row') # contents에서 key가 key_nm 인 값을 가져오고, 그값에서 또 key가 row인 값들을 가져와라
            row_df = pd.DataFrame(row_data)

            return row_df
        

