select count(*) from people_status;
select distinct id from people_status;
select status_id, count(people_id) from people_status group by status_id order by status_id;
select people_id, status_date from people_status where  status_id=8;
select * from people where retire_date IS NOT NULL;

select people.id, concat(people.first_name, ' ', people.last_name) AS name 
from people
    JOIN people_status ON people.id = people_status.people_id  
where  status_id=8;




select people_status.status_date, statuses.name  
from statuses
    JOIN people_status ON statuses.id = people_status.status_id  
where  people_id=60
ORDER BY people_status.status_date;


select people.id, concat(people.first_name, ' ', people.last_name) AS name, people_status.status_date, statuses.name 
from people
    JOIN people_status ON people.id = people_status.people_id 
    JOIN statuses ON statuses.id = people_status.status_id 
where  people_status.people_id  IN (
        SELECT people_id FROM (
            SELECT people_id, COUNT(people_id) AS pc
            FROM people_status
            GROUP BY people_id
        
        ) AS PINUS
        WHERE pc> 2 
    ) as anus
ORDER BY people_status.people_id, people_status.status_date;


select * from (
    SELECT people_id FROM (
                SELECT people_id, COUNT(p.people_id) AS pc
                FROM people_status
                GROUP BY people_id
                ) AS PINUS 
    WHERE pc> 2 ) AS anus;
    
select MAX(events) from (
    SELECT people_id, count(status_id) as events 
    FROM 
        people_status
        GROUP BY people_id
        ) AS PINUS ;


SELECT EXISTS (
	SELECT people_status.id, people_status.people_id, people_status.status_id, people_status.status_date 
	FROM people 
	JOIN people_status ON people.id = people_status.people_id 
	JOIN statuses ON statuses.id = people_status.status_id 
	WHERE people_status.status_id IN (
		SELECT anon_2.id 
		FROM (
			SELECT statuses.id AS id 
			FROM statuses 
			WHERE statuses.name IN (__[POSTCOMPILE_name_1])
			) AS anon_2)
		) AS anon_1