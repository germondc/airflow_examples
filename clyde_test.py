from datetime import timedelta
from collections import defaultdict

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import cross_downstream
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'test_clyde',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

dag.doc_md = __doc__	

template_clyde = """
echo "\$0: $0"
echo "ps: $$"
whoami
echo "sleep_length: {{ params.sleep_length }}"
echo "message: {{ params.message }}"
echo "ds: {{ ds }}"
echo "ds_nodash: {{ ds_nodash }}"
echo "task_id: {{ task.task_id }}"
echo "base_url: {{ conf.get('webserver', 'BASE_URL') }}"
sleep {{ params.sleep_length }}
"""

def template_clyde_success(context):
    print("success callback")
    print("dag_id: " + context['dag'].dag_id)
    print("task_id: " + context['task_instance'].task_id)
    
def template_clyde_failure(context):
    print("failure callback")
    print("dag_id: " + context['dag'].dag_id)
    print("task_id: " + context['task_instance'].task_id)
    print("params: ")
    print(context['params'].items())
    print("next_ds: " + context['next_ds'])
    print("ds_nodash: " + context['ds_nodash'])

t4 = BashOperator(
    task_id='clyde',
    depends_on_past=False,
    bash_command=template_clyde,
    params={
    	'sleep_length': '1',
    	'message': 'hello'
    },
    dag=dag,
    on_success_callback=template_clyde_success,
    on_failure_callback=template_clyde_failure,
    execution_timeout=timedelta(seconds=3),
)

def generate_bash_operator(i, message):
    return BashOperator(
        task_id='task_{0}'.format(i),
        bash_command='echo {0}'.format(message),
        dag=dag,
    )

d1 = DummyOperator(
    task_id='entry',
    dag=dag
)

#for i in range(0, 5):
#    d2 = generate_bash_operator(i, "this is the message")
#    d1 >> d2
    
with open("temp.txt", "r") as f:
    lines = f.read().splitlines()

d = defaultdict(list)
id = 0
for x in lines:
    if (x.strip().startswith("#")):
         continue
    split = x.split(" ")
    if (len(split) != 2):
        continue

    level = split[0]
    if (not level.isdecimal()):
        continue
    level = int(level)

    item = split[1]
    d2 = generate_bash_operator(id, item)
    d[level].append(d2)
    id = id+1
    
prev = [d1]
for l in sorted(d.items()):
    cross_downstream(prev, l[1])
    prev = l[1]
