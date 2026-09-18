with ticks as (
    select * from {{ ref('stg_price_ticks') }}
),

ativo as (
    select * from {{ ref('dim_ativo') }}
),

tempo as (
    select * from {{ ref('dim_tempo') }}
)

select
    ticks.price_tick_id,
    ativo.ativo_id,
    tempo.tempo_id,
    ticks.avg_price,
    ticks.price_volatility,
    ticks.min_price,
    ticks.max_price,
    ticks.total_volume,
    ticks.tick_count,
    -- variação percentual dentro da própria janela, usada pelo teste de anomalia
    safe_divide(ticks.max_price - ticks.min_price, ticks.min_price) * 100 as price_range_pct
from ticks
left join ativo on ticks.symbol = ativo.symbol
left join tempo on ticks.window_start = tempo.window_start
