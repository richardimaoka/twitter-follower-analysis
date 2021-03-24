BQ users

SELECT * FROM users

pub userId
sub userId
  -> twitter api, save to bq
    -> pub next page
    -> sub next page
     -> twitter api, save to bq
     -> pub next page
     ...
  -> pub next user cursor

  
