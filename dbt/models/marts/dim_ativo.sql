with distinct_symbols as (
    select distinct symbol
    from {{ ref('stg_price_ticks') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} as ativo_id,
    symbol,
    case
        when symbol like '%USDT' then 'cripto'
        else 'acao'
    end as classe_ativo
from distinct_symbols
