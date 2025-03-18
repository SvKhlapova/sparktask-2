# Задания по Spark Advanced и Realtime

#### Комментарий

Задачу 2 выполнять только на Dstream API (аналог RDD для Spark Streaming).

## Задача 2. Spark Streaming
#### Исходные данные
Входные данные: <pre>/data/realtime/uids</pre>

Формат данных:
<pre>
...
seg_firefox 4176
...
</pre>

- /data/wiki/stop_words_en-xpo6.txt - список стоп-слов для 2-й задачи.

### Условие  
Сегмент - это множество пользователей, определяющееся неким признаком. Когда пользователь посещает web-сервис со своего устройства, это событие логируется на стороне web-сервиса в следующем формате: <span style="background-color: lightgray;">user_id <tab> user_agent</span>. Например:
<pre>
f78366c2cbed009e1febc060b832dbe4	Mozilla/5.0 (Linux; Android 4.4.2; T1-701u Build/HuaweiMediaPad) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.73 Safari/537.36
62af689829bd5def3d4ca35b10127bc5	Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36
</pre>  

На вход поступают порции web-логов в описанном формате. Требуется разбить аудиторию (пользователей) в этих логах на следующие сегменты:

1. Пользователи, которые работают в интернете из-под IPhone.
2. Пользователи, кот. используют Firefox браузер.
3. Пользователи, кот. используют Windows.  
#
Не стоит волноваться если какие-то пользователи не попадут ни в 1 из указанных сегментов поскольку в реальной жизни часто попадаются данные, которые сложно классифицировать. Таких пользователей просто не включаем в выборку.
Также сегменты могут пересекаться (ведь возможен вариант, что пользователь использует Windows, на котором стоит Firefox). Для того, чтоб выделить сегменты можно использовать следующие эвристики (или придумать свои):

| Сегмент       | Эвристика                                           |
|---------------|-----------------------------------------------------|
| seg_iphone    | parsed_ua['device']['family'] like '%iPhone%'       |
| seg_firefox   | parsed_ua['user_agent']['family'] like '%Firefox%'  |
| seg_windows   | parsed_ua['os']['family'] like '%Windows%'          |


Оцените кол-во уникальных пользователей в каждом сегменте используя алгоритм [HyperLogLog](https://github.com/svpcom/hyperloglog) (поставьте <span style="background-color: lightgray;">error_rate</span> равным 1%). 
В результате выведите сегменты и количества пользователей в следующем формате: <span style="background-color: lightgray;">segment_name <tab> count</span>. Отсортируйте результат по количеству пользователей в порядке убывания.

#### Код для генерации батчей

В задаче используйте его без изменений т.к. он критичен для системы проверки.

<pre>
from hdfs import Config
import subprocess

client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn-client')

# Preparing base RDD with the input data
DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

# Creating QueueStream to emulate realtime data generating
BATCH_TIMEOUT = 2 # Timeout between batch generation
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)
</pre>

