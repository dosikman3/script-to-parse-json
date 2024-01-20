import requests
import json
import psycopg2
from psycopg2.extras import execute_values


def get_token_and_dataset():
    # Получем токен через запрос на сервер с логином и паролем.
    url = 'https://your_url/token'
    client_id = 'your_client_id'
    client_secret = 'your_client_secret'
    grant_type = 'your_grant_type'

    body = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': grant_type,
    }

    response = requests.post(url, data=body)
    response_json = response.json()
    access_token = response_json.get('access_token')

    # Получаем Датасет
    url = 'https://your url/'
    Authorization = {
        'Authorization': f'Bearer {access_token}'
    }

    payload = {
        """
        Пишите свой пост запрос
        """
    }

    response = requests.post(url, data=payload, headers=Authorization)
    response_dict = response.json()
    dataset = json.dumps(response_dict, indent=4, ensure_ascii=False).encode('utf-8').decode('utf-8')
    # Записываем в json файл
    with open('output.json', 'w', encoding='utf-8') as json_file:
        json_file.write(dataset)
    return True


def bd_connection():
    # Подключение к БД и отправка данных в БД
    conn = psycopg2.connect(
        database="your Database",
        user="your user",
        password="your password",
        host="your host",
        port="ypur port"
    )

    cur = conn.cursor()

    with open('output.json', 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    table_name = "your table_name"

    check_table_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
    cur.execute(check_table_query)
    table_exists = cur.fetchone()[0]

    if not table_exists:
        print(f"Таблица {table_name} не существует. Создайте таблицу перед вставкой данных.")
    else:
        # В зависимости какой у вас Датасет и что в него вложено сами регулируете запрос
        for row_list in data["ypur dataset"]["your data"]:
            for row in row_list:
                row_values = [value if value is not None else None for value in row.values()]
                insert_query = f"INSERT INTO {table_name} ({', '.join(['\"' + key + '\"' for key in row])}) VALUES %s"
                execute_values(cur, insert_query, [tuple(row_values)])

        conn.commit()
        print("Данные были успешно вставлены")

    cur.close()
    conn.close()


def main():
    # Пока данные не будут сохранены в json он не будет отправлять данные в БД
    result = get_token_and_dataset()
    if result:
        print("Данные были сохранены в output.json.")
        bd_connection()
    else:
        print("Ошибка.")


if __name__ == "__main__":
    main()
