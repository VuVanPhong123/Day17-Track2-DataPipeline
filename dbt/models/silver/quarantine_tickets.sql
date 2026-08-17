-- ---------------------------------------------------------------------------
-- quarantine_tickets — nơi tiếp nhận bản ghi CDC không thoả data contract.
-- ---------------------------------------------------------------------------
-- Grain: 1 hàng / 1 BẢN GHI CDC bị loại — không phải 1 hàng / 1 ticket.
-- Điều kiện dùng cùng normalize_priority() với silver_tickets để bảo đảm hai
-- phía không thể lệch logic: row nào không normalize được thì rơi vào đây.
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

select
    ticket_id,
    cdc_seq,
    op,
    event_time,
    _ingested_at,
    priority_raw,
    {{ priority_reject_reason('priority_raw') }}             as reject_reason,
    customer_id,
    customer_name,
    category,
    status
from {{ source('bronze', 'bronze_tickets_cdc') }}
where {{ normalize_priority('priority_raw') }} is null
