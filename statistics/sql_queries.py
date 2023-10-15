import mysql.connector

grant = """
grant all privileges on workers.* to "posmotret"@"localhost" 
"""
cursor = cnx.cursor()
cursor.execute(grant)

cursor.execute("select user from mysql.user")
cnx.close()



def count_people(cursor):
    '''
    считает количество записей в таблице people, то есть количество людей
    '''
    quer = '''
    SELECT COUNT(*) 
    FROM people 
    '''
    cursor.execute(quer)
    l = cursor.fetchall()
    for i in l:
        print(i)

def life_long(cursor):
    quer = '''
    select *, from_days(datediff(death_date, birth_date)) as life, death_date - birth_date as absurd  
    FROM people 
    WHERE retire_date IS NOT NULL;
    '''
    cursor.execute(quer)
    l = cursor.fetchall()
    for i in l:
        print(i)

def mean_qualification(cursor):
    # средняя квалификация по всем людям
    print('=' * 20)
    quer = '''
    SELECT AVG(current_position_id) FROM people 
    '''
    cursor.execute(quer)
    avg_pos = cursor.fetchone()[0]
    print(avg_pos, type(avg_pos))
    avg_pos = float(avg_pos)
    print(f"Среняя квалификация людей: {avg_pos:5.3f}")


def talent_mean_qualification(cursor):
    # считает среднюю должность в завистмости от таланта человека.
    # Выводит так же количество люедй с определенным значением таланта
    quer = """
        SELECT talent, count(talent) as people_with_talent, 
	        CAST(avg(current_position_id) AS FLOAT) AS avg_position
        FROM people
        group by talent
        order by talent;
    """
    cursor.execute(quer)
    l = cursor.fetchall()
    for i in l:
        print(i)

def talent_mean_qualification_windowed_func(cursor):
    # Считает среднюю должность в зависимости от таланта человека с использованием оконных функций.
    # Выводит так же количество людей с определенным значением таланта.
    # А так же количество людей на конкретной должности с определенным значением таланта
    quer = """
        SELECT row_number() over(ORDER BY talent, current_position_id) AS num, 
            id, talent, current_position_id,
            count(id) over(partition by talent, current_position_id) as num_position_with_talent,
            count(talent) over(partition by talent) as people_with_talent, 
            CAST(avg(current_position_id) over(partition by talent) as float) AS avg_position
        FROM people
        order by talent, current_position_id;
    """
    cursor.execute(quer)
    l = cursor.fetchall()
    for i in l:
        print(i)


cursor = cnx.cursor()
# count_people(cursor)
# life_long(cursor)
# mean_qualification(cursor)
# talent_mean_qualification(cursor)
talent_mean_qualification_windowed_func(cursor)
cnx.close()