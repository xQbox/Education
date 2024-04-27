import pandas as pd
import datetime as dt
import os

def myFuncRelease(path:str) -> pd.DataFrame:
    """
    Recieve value
    :param path: - path to file.csv\n
    Make\n
    :output: a file "output.csv" in current dir \n
    Return value \n
    : Success 
    : - pd.DataFrame \n
    : Error: 
    : - 1 -> Invalid FilepPath error occur\n
    : - 2 -> Invalid DataRead error occur\n
    """
    try:
        df = pd.read_csv(
        filepath_or_buffer=path,
        sep="\t",
        parse_dates=['timestamp']
        )
    except FileNotFoundError:
        return 1
    
    # Новые колонки
    try:
        df['date'] = pd.to_datetime(df['timestamp'].dt.strftime("%Y-%m-%d")) 
    except AttributeError:
        return 2
    
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day

    start_date = df.timestamp.min()
    end_date = df.timestamp.max()

    # Убираем первые и последние месяцы
    df = df[~((df['timestamp'].dt.year == start_date.year) & (df['timestamp'].dt.month == start_date.month))]
    df = df[~((df['timestamp'].dt.year == end_date.year) & (df['timestamp'].dt.month == end_date.month))]

    # Определим дату регистрации для каждого пользователя (дату первого действия на сайте)
    reg_dates = df.groupby(['userid'])['date'].agg(['min']).reset_index().rename(columns={'min':'reg_date'})

    # Добавляем дату регистрации для каждого пользователя
    df = df.merge(reg_dates, left_on='userid', right_on='userid', how='left')

    # Выберем только те записи, когда были покупки, совершенные в день первого визита на сайта
    reg_dates_confirmation = df.query('action == "confirmation" & date == reg_date')

    # Считаем максимальные продажи в каждый из дней
    result = reg_dates_confirmation.groupby('date')['value'].agg('max')
    result = pd.DataFrame(result).reset_index()

    # Колонки для группировки
    result['year'] = result['date'].dt.year
    result['month'] = result['date'].dt.month
    result['day'] = result['date'].dt.day

    # Дропаем если есть одинаковые сумму в рамках одного месяц
    # Дропаются повторяющиеся, а первые останутся, так как у нас уже отсортированный по возрастанию датасет
    # И будут оставаться только самые ранние даты в случае наличия дубликатов по year-month-value
    result = result.drop_duplicates(subset=['year', 'month', 'value'])

    # Найдем индексы с максимальными значениями по каждому месяцу
    idx = pd.DataFrame(result.groupby(['year', 'month'])['value'].idxmax()).reset_index()['value'].to_list()

    # Вытащим из результатов записи с максимальными суммами покупки в каждом месяце
    output = result.iloc[idx,[0,1]].rename(columns={'date':'timestamp'})

    # Сохранение
    output.to_csv('output.csv', sep='\t', index=False)

    return output

# myFuncRelease('data_for_testing/variant35.csv')