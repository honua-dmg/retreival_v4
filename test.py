
import Main
import wbsoc
import redis
import tracemalloc
import datetime as dt
import time
r = redis.Redis(host="localhost",port="6379",db=0)
hours, mins = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d:: %H:%M").split(':')
if int(hours)<3 or (int(hours)>=3 and int(mins)<30):
    r.flushall()
token ='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhcGkuZnllcnMuaW4iLCJpYXQiOjE3MzU3ODkxNDcsImV4cCI6MTczNTg2NDIwNywibmJmIjoxNzM1Nzg5MTQ3LCJhdWQiOlsieDowIiwieDoxIiwieDoyIiwiZDoxIiwiZDoyIiwieDoxIiwieDowIl0sInN1YiI6ImFjY2Vzc190b2tlbiIsImF0X2hhc2giOiJnQUFBQUFCbmRncGI1b3NsNmZQbmVoaGRfZGNjc3cyaS13eEtQMTgxRXBnY3QxM1pPM21iSno2M0VUZV84bGFGZDhYckVDSmpVUmkwc0tIaEpZRncydEFiT1JmSk1YcVJtbkN1VmEtM0RHX3RHYWhjdXN5dnMtYz0iLCJkaXNwbGF5X25hbWUiOiJNQUxMRVBBTExJIEdBSlVMQSBHVVJVIFNBSSBQUkFTQUQiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiJiNGM5NzE0ZmY3ZjViYTBjNWYzMGRlYTEyOWIxY2Q1MGFkYjZkOTllZjk1MzhhMWVhMTVjMmUyYiIsImZ5X2lkIjoiWU0wODkyNyIsImFwcFR5cGUiOjEwMCwicG9hX2ZsYWciOiJOIn0.Md1mRsrcy3RsGaBf4k4pvGgbksFk4a0ngs7uKL6IlK8'
tracemalloc.start()
Main.threadripper(token,True)

with open("lawg.txt",'a') as f:
    print(f"{dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()),'%H-%M-%S')}: memory used:{tracemalloc.get_traced_memory}",file=f)