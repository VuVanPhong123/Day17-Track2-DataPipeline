# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** [điền họ tên]  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 38.8s
run 2/3 … 36.9s
run 3/3 … 35.5s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
  số file parquet                           ✗ 5,000 → 5,000
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**. Các dòng dashboard thuộc bài mở rộng trong `EXTRA.md`, không nằm trong ba nhiệm vụ bắt buộc.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng sau mỗi lần rerun; baseline của lab đạt 38.750 hàng thay vì 12.480 và cùng một `ticket_id` xuất hiện nhiều lần. |
| **Nguyên nhân** | Model là incremental nhưng không có `unique_key` và strategy phù hợp, nên dbt ghi theo kiểu append. Khi Airflow retry/Clear Task phát lại cùng partition, các entity đã tồn tại bị chèn thêm. Với CDC còn có `op='u'`, cùng một ticket có thể xuất hiện ở nhiều ngày, nên xử lý theo partition ngày không bảo đảm grain `1 ticket = 1 row`. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: đặt `unique_key='ticket_id'`, `incremental_strategy='merge'`. `dags/ai_training_pipeline.py`: đặt `catchup=False`, `max_active_runs=1` để hạn chế replay/concurrent run; đây là hardening, không thay thế tính idempotent của model. |
| **Bằng chứng** | Sau sửa: **12.480 / 12.480** hàng, không ticket lặp; checksum ba lượt đều **`8dd7c98653`**. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 hàng thay vì 9.100; các cặp `(event_date, customer_id)` thiếu tập trung ở ngày quá khứ dù dữ liệu đã tới Bronze/Silver. |
| **P99 độ trễ đo được** | **2.725833 ngày** (~65,42 giờ). P50 = 0.128090 ngày, P95 = 1.813693 ngày, max = 2.944688 ngày; tỷ lệ trễ hơn 1 ngày ≈ **5,05%**. |
| **Lookback đã chọn** | **3 ngày** — `ceil(P99) = 3`; với dataset hiện tại còn bao phủ cả max quan sát 2.944688 ngày. |
| **Nguyên nhân** | Incremental filter cũ dùng `event_date > max(event_date)` của target. Một event xảy ra ở ngày cũ nhưng đến kho 2–3 ngày sau luôn có `event_date <= max(event_date)`, nên không bao giờ được tính lại. Chỉ đổi `>` thành `>=` vẫn chỉ mở lại khoảng một ngày và không đủ cho phân bố độ trễ đo được. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: tính lại cửa sổ 3 ngày bằng `event_date >= max(event_date) - interval 3 day`; thêm composite `unique_key=['event_date','customer_id']` và `incremental_strategy='merge'` để recompute window không tạo duplicate. |
| **Bằng chứng** | Trước: **8.645** hàng · sau: **9.100 / 9.100**; checksum ba lượt đều **`3db448685c`**. |

Chọn P99 làm căn cứ vì nó mô tả gần như toàn bộ phân bố thực tế nhưng ít bị một outlier cực đoan kéo window tăng vô hạn. Mỗi ngày lookback tăng làm model phải quét và aggregate lại thêm dữ liệu ở **mọi** incremental run, nên chi phí compute tăng theo window. Trong dataset này, làm tròn P99 lên 3 ngày đồng thời bao phủ cả max quan sát; nếu max là outlier rất lớn thì dùng max sẽ gây chi phí lặp lại không tương xứng.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Sau khi nguồn đổi `priority` từ số sang nhãn chữ, pipeline vẫn chạy nhưng `try_cast` tạo nhiều NULL; đồng thời các chuỗi số ngoài miền như `0`, `5`, `-1` lại vẫn cast thành integer và lọt qua. |
| **Nguyên nhân** | Hệ thống chỉ ép kiểu mà chưa thực thi semantic contract. `urgent/high/medium/low` là biểu diễn mới nhưng hợp lệ, trong khi `P1`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng và NULL mới là dữ liệu lỗi. Contract ban đầu chưa enforced. Ngoài ra, nếu rank CDC trước rồi mới bỏ bản ghi lỗi thì ticket có update mới nhất bị lỗi sẽ mất luôn trạng thái hợp lệ trước đó. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `1..4` → giữ nguyên; `urgent/high/medium/low` → map lần lượt `1/2/3/4`; giá trị còn lại/out-of-range/rỗng/NULL → trả NULL và route sang quarantine. |
| **Cách khắc phục** | `normalize_priority.sql`: CASE normalize theo contract; `silver_tickets.sql`: loại record không normalize được **trước** `row_number()` rồi mới chọn trạng thái mới nhất; `quarantine_tickets.sql`: lấy đúng các raw record normalize ra NULL; `schema.yml`: bật `contract.enforced: true`, thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 / 312**; `silver_tickets.priority` sạch và luôn ∈ 1..4; Silver vẫn giữ đủ **12.480** ticket; `dbt test` **11/11 pass**. |

Nên giữ Bronze nguyên bản để bảo toàn payload nguồn và thực hiện validation/normalization ở Silver. Không nên để vài trăm record lỗi dừng toàn pipeline vì chúng không được phép chặn các event/transcript hợp lệ khác; thay vào đó record lỗi được quarantine để điều tra và có thể replay sau khi sửa dữ liệu hoặc contract.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm — ba nhiệm vụ bắt buộc đã đạt 4/4. |
| **Nguyên nhân** | Dashboard small-file/partition và consumer delivery semantics là bài thưởng, không thuộc yêu cầu 100 điểm chính. |
| **Cách khắc phục** | Không áp dụng trong phạm vi nộp bài chính. |
| **Bằng chứng** | `make verify` xác nhận **4/4 tiêu chí bắt buộc đạt**; dashboard giữ nguyên result hash nhưng chưa tối ưu scan/file count. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain/natural key và semantics của incremental write/retry trước khi tin rằng job rerun là an toàn. |
| 2 | So sánh event time với ingestion time, đo percentile độ trễ rồi mới chọn watermark/lookback. |
| 3 | Tách schema/type contract khỏi semantic value contract, kiểm tra thứ tự validate → dedup/rank và luôn có đường quarantine cho bad records. |
