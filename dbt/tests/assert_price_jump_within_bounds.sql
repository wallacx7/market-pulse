-- Falha (retorna linhas) quando um ativo varia mais que 10% dentro da mesma
-- janela de 1 minuto. Um teste dbt que "falha" aqui não é um erro de pipeline
-- — é um sinal de negócio (possível anomalia de mercado) que o agente de IA
-- consome depois para investigar a causa.
select
    price_tick_id,
    ativo_id,
    tempo_id,
    price_range_pct
from {{ ref('fct_price_ticks') }}
where price_range_pct > 10
