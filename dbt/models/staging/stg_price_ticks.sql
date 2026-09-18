with source as (
    select * from {{ source('silver', 'price_aggregates') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['symbol', 'window_start']) }} as price_tick_id,
        symbol,
        window_start,
        window_end,
        avg_price,
        price_volatility,
        min_price,
        max_price,
        total_volume,
        tick_count
    from source
)

select * from renamed
