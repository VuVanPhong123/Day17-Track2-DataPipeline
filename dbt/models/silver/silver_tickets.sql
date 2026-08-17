-- ---------------------------------------------------------------------------
-- silver_tickets — trạng thái mới nhất của mỗi ticket, dựng lại từ luồng CDC.
-- ---------------------------------------------------------------------------
-- Model này materialized = 'table': dựng lại toàn bộ mỗi lần chạy, nên luôn
-- ổn định.
--
-- Phần xử lý CDC dưới đây giữ đúng semantics:
--   * loại bản ghi có priority không hợp lệ trước khi xếp hạng
--   * mỗi ticket lấy bản ghi hợp lệ có (event_time, cdc_seq) lớn nhất
--   * ticket có op = 'd' bị loại khỏi Silver
--
-- Việc lọc trước row_number là quan trọng: nếu update mới nhất bị hỏng, ticket
-- vẫn phải giữ trạng thái hợp lệ gần nhất thay vì biến mất hoàn toàn.
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

with normalized as (

    select
        *,
        {{ normalize_priority('priority_raw') }} as priority_clean
    from {{ source('bronze', 'bronze_tickets_cdc') }}

),

valid as (

    select *
    from normalized
    where priority_clean is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from valid

),

latest as (
    select * from ranked where _rn = 1
)

select
    ticket_id,
    customer_id,
    customer_name,
    segment,
    priority_clean                                           as priority,
    category,
    channel,
    status,
    csat,
    first_response_sec,
    subject,
    body,
    event_time                                               as updated_at,
    _ingested_at
from latest
where op <> 'd'
