{{ config(materialized='view') }}

with source_data as (

    select R.*, L.color
    from data-practice-348620.basketball.mascots as R left join data-practice-348620.basketball.team_colors as L
    on R.market = L.market

)

select *
from source_data