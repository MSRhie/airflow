from datetime import datetime
from dateutil import relativedelta

now = datetime(year=2023, month=3, day=30)
print("현재시간:"+str(now))
print('--------------월 연산 --------------')
# 1월로 변경
print(now + relativedelta.relativedelta(month=1)) 
# 1월로 변경
print(now.replace(month=1)) 
# 위의 day와 다름(days임) 1개월 뺴기
print(now + relativedelta.relativedelta(months=-1)) 
print('--------------연산 여러 개--------------')
 # 2020년 1월로 변경
print(now + relativedelta.relativedelta(months=-1) + relativedelta.relativedelta(days=-1))