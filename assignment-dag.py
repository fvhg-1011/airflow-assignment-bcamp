from airflow.decorators import dag, task_group, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.operators.python import PythonOperator

list_table = Variable.get("mysql_to_hive", deserialize_json=True)


# fungsi transform ubah yg huruf awal yg kecil jadi huruf besar (bisa diganti jadi apa aja sebenernya)
def transform_logic(data):
    transformed_data = []
    for row in data:
        transformed_row = tuple(
            value.capitalize() if isinstance(value, str) else value for value in row
        )
        transformed_data.append(transformed_row)
    return transformed_data


@dag()
def test_assignment():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    for table in list_table:
        group_id = table.replace(".", "__")

        @task_group(group_id=group_id)
        def group():
            get_schema = MySqlOperator(
                task_id="get_schema",
                mysql_conn_id="mysql_default",
                sql=f"DESC {table}",
            )
            extract = MySqlOperator(
                task_id="extract_from_mysql",
                mysql_conn_id="mysql_default",
                sql=f"SELECT * FROM {table}",
            )

            # transform data ambil dari function transform logic
            def transform_data(ti):
                data = ti.xcom_pull(task_ids=f"{group_id}.extract_from_mysql")
                transformed_data = transform_logic(data)
                return transformed_data

            transform = PythonOperator(
                task_id="transform_data",
                python_callable=transform_data,
            )

            # buat create table otomatis
            @task(task_id="create_hive_table")
            def create_hive_table(ti):
                schema = ti.xcom_pull(task_ids=f"{group_id}.get_schema")
                columns = ", ".join([f"{column[0]} {column[1]}" for column in schema])
                hql = f"CREATE TABLE IF NOT EXISTS {table} ({columns})"
                hive_hook = HiveCliHook()
                hive_hook.run_cli(hql)

            load = HiveOperator(
                task_id="load_to_hive",
                hive_cli_conn_id="hive_cli_default",
                hql="""
                    INSERT INTO """
                + table
                + """ (
                        {% for column in ti.xcom_pull(task_ids='"""
                + group_id
                + """.get_schema') %}
                            {{ column[0] }}{{ ', ' if not loop.last else '' }}
                        {% endfor %}
                    )
                    VALUES
                        {% for data in ti.xcom_pull(task_ids='"""
                + group_id
                + """.transform_data') %}
                            (
                                {% for column in data %}
                                    {{ "'"~column~"'" if column is string else column }}{{ ', ' if not loop.last else '' }}
                                {% endfor %}
                            ){{ ', ' if not loop.last else '' }}
                        {% endfor %}
                """,
            )
            (
                start_task >> get_schema >> extract >> transform >> create_hive_table() >> load >> end_task
            )

        group()


test_assignment()
