with distinct_windows as (
    select distinct window_start
    from {{ ref('stg_price_ticks') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['window_start']) }} as tempo_id,
    window_start,
    extract(date from window_start) as data,
    extract(hour from window_start) as hora,
    extract(minute from window_start) as minuto,
    extract(dayofweek from window_start) as dia_semana
from distinct_windows
