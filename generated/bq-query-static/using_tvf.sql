declare myId default '1';

with someprojectmydatasetsourcetable as (select 'id' as id)

select id from someprojectmydatasetsourcetable where id = myId
          