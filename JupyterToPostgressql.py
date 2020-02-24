import json
import glob
import os
import psycopg2

con = psycopg2.connect(
  database="Jupyterlab",
  user="bagzhan",
  password="qwerty",
  host="127.0.0.1",
  port="5432"
)

print("Database opened successfully")
cur = con.cursor()

ipynbfiles=[]
for file in glob.glob('myPath\\*.ipynb'):    # В myPath пишем путь где находятся файлы *.ipynb с последующим парсингом данных из них
    ipynbfiles.append(file)


for filename in ipynbfiles:
    with open(filename, encoding='utf-8') as json_file: # Открывает *.ipynb папку
        data = json.load(json_file)
        data2 = data['cells']           # берет из json определенный ключ с названием 'cells'
        text = ['text/plain', 'text/html']   # Ключи внутри словаря 'data' для сортировки нужных данных
        for i in data2:
            input_Output = {'Input':None,'Output':None}        # Создает словарь ключи без значения для отправки данных в Postgresql
            input = []                  # Список для входных данных
            output = []                 # Список для выходных данных
            for num, value in enumerate(i['source']):
                input.append(value)
                input_Output['Input'] = input
            for num, value in enumerate(i['outputs']):
                if 'text' in value:
                    input_Output['Output'] = str(value['text'])[1:-1]     # Сохроняет значения данных под названием 'text' в словарь
                elif 'data' in value:
                    #resultAny = any(elem in text for elem in value['data'])
                    resultAll = all(elem in text for elem in value['data'])     # Проверяет есть ли нужный ключ в словаре 'data'
                    for n,key in enumerate(value['data']):
                        if resultAll:       # Если есть оба ключа c названиями ('text/plain','text/html') в json под ключом 'data', то сохроняет значения этого ключа
                            output.append(str(value['data'][key])[1:-1])  # Сохроняет выходные данные в список последующим добавлением в словарь значением под ключ 'Output'
                        else:
                            output.append(str(value['data'][key])[1:-1])  # Сохроняет выходные данные в список последующим добавлением в словарь значением под ключ 'Output'
                    input_Output['Output'] = output
            print(input_Output)
            cur.execute("INSERT INTO jupyterlab(Input, Output) VALUES (%s,%s)", (input_Output['Input'],input_Output['Output']))   # Отправка Входных и Выходных данных в таблицу

print("Table commited successfully")
cur.close()
con.commit()
con.close()
