# Лабораторная: статистические особенности языка (цепи Маркова)

## Описание

Скрипт на Python считывает большой текстовый файл (русский текст),
нормализует его (оставляет только русские буквы в нижнем регистре,
пробел, точку, запятую, восклицательный и вопросительный знаки), и
считает:

- глобальные частоты всех символов;
- частоты появления символов после префиксов длиной от 1 до 13.

Результаты сохраняются в базу данных PostgreSQL в следующие таблицы:

- `global_freqs(symbol, count, prob)`
- `transitions_1(prefix, next_symbol, count, prob)`
- ...
- `transitions_13(prefix, next_symbol, count, prob)`

## Настройка

1. Положить исходный текстовый файл в папку `data/` под именем `text.txt`.
2. В файле `src/config.py` указать корректные настройки подключения к PostgreSQL.

## Запуск

```bash
python -m venv venv
source venv/bin/activate  # или .\venv\Scripts\Activate.ps1 в Windows
pip install -r requirements.txt
cd src
python main.py
