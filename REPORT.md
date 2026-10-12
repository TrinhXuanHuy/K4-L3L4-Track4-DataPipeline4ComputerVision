# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trịnh Xuân Huy |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602995` |
| GitHub | `TrinhXuanHuy` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07304` |

## Kết quả cuối

- `dataset_version` của bạn: `65ebc8820851`
- Số khung hình train / val / test của bạn: `train: 702, val: 157, test: 160 (tổng: 1019)`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

- **Ít nhất 4 khác biệt giữa nguồn A và nguồn B ở Bronze:**
  1. *Timestamp*: Nguồn A dùng `ts_utc` định dạng chuỗi chuẩn ISO 8601 (ví dụ `2025-10-07T08:00:00.014Z`), trong khi nguồn B dùng `timestamp_ms` dạng số nguyên Unix epoch mili-giây (ví dụ `1759933801509`) và có độ lệch đồng hồ (+1500 ms) so với video.
  2. *Tốc độ (Speed)*: Nguồn A dùng cột `speed_mps` (đơn vị mét/giây m/s), nguồn B dùng cột `spd_kmh` (đơn vị km/h).
  3. *Phương vị (Heading)*: Nguồn A đặt tên cột là `heading_deg`, nguồn B đặt tên cột là `hdg`.
  4. *Tọa độ & Độ cao*: Nguồn A dùng `lat`, `lon`, `alt_m`; nguồn B dùng `latitude`, `longitude`, `altitude`.
  5. *Tín hiệu điều khiển & Gia tốc*: Nguồn A dùng `gas`, `brake`, `steer`, `acc_x`, `acc_y`; nguồn B dùng `throttle_pct` (thang %), `brake_pct`, `steering`, `ax`, `ay`.
- **Lý do giữ mỗi nguồn một bảng bronze và để mọi cột dạng chữ:**
  - Tầng Bronze tuân thủ nguyên tắc thiết kế Data Lakehouse: *Lossless & Append-only (nguyên bản, chỉ thêm)* để đóng vai trò làm Single Source of Truth tuyệt đối.
  - Cấu trúc dữ liệu giữa 2 nguồn hoàn toàn dị biệt (schema drift). Nếu gộp chung ngay từ Bronze sẽ tạo ra bảng hỗn tạp với nhiều cột NULL dư thừa hoặc nhầm lẫn ngữ nghĩa giữa các đơn vị đo lường.
  - Lưu dạng chuỗi Text (String) cho toàn bộ các cột đảm bảo việc nạp dữ liệu (Ingestion) không bao giờ bị crash do lỗi ép kiểu (parsing exception), kể cả khi xuất hiện giá trị dị biệt, ô trống hay dữ liệu bẩn. Việc làm sạch, ép kiểu và chuẩn hóa được chuyển sang tầng Silver theo đúng mô hình ELT hiện đại.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

- **Cách pipeline đưa hai nguồn về một schema chung:**
  - *Đơn vị tốc độ*: Chuẩn hóa toàn bộ về $m/s$. Nguồn A giữ nguyên `speed_mps`, nguồn B quy đổi từ `spd_kmh / 3.6`.
  - *Kiểu thời gian*: Thống nhất về kiểu UTC Timestamp `TIMESTAMP WITH TIME ZONE`. Nguồn A parse chuỗi ISO string, nguồn B chuyển đổi từ mili-giây epoch bằng `to_timestamp(timestamp_ms / 1000)`.
  - *Độ lệch đồng hồ (Clock Offset)*: Nguồn B có độ lệch cảm biến 1500 ms (+1.5 giây) so với video; pipeline tự động trừ đi độ lệch này dựa trên metadata cấu hình `telemetry_clock_offset_ms` trong bảng `bronze_video_files`.
- **Bằng chứng kết quả Bước 2 đã chuẩn hóa đúng:**
  - Điểm bắt đầu của Drive B sau khi trừ offset là `2025-10-08 14:30:00.009000+00:00`, khớp với thời điểm bắt đầu của video (< 0.5s): kiểm tra `PASS drive B clock offset removed (first sample within 0.5 s of video start)`.
  - Tốc độ tối đa sau khi quy đổi của cả 2 xe đều hợp lý trong điều kiện giao thông thực tế: Drive A đạt 71.0 km/h, Drive B đạt 100.0 km/h (đều nằm trong ngưỡng vật lý < 130 km/h hay < 70 m/s).
  - Định luật bảo toàn bản ghi: `clean + quarantined == received` đạt tuyệt đối ở cả 2 nguồn (Drive A: $2968 + 53 = 3021$; Drive B: $3181 + 53 = 3234$).
- **Bảng Quarantine (dòng lỗi theo từng lý do, từng drive):**

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

- **Vì sao dòng lỗi vào `rejects` thay vì bị xóa:**
  - Đảm bảo tính minh bạch và khả truy (*Auditability & Observability*): Giữ lại các dòng lỗi kèm lý do phân loại (`reject_reason`) giúp đội ngũ Data/ML Engineer dễ dàng điều tra lỗi phần cứng, cảm biến hoặc mạng truyền dẫn.
  - Tránh mất mát dữ liệu âm thầm (*No Silent Data Loss*): Nếu xóa trực tiếp, tổng số bản ghi không còn khớp với nguồn gốc, khiến việc kiểm toán thất bại; việc cách ly vào quarantine cho phép khôi phục hoặc reprocessing lại dữ liệu nếu sau này các quy tắc nghiệp vụ thay đổi.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`:**
  - `max_gap_ms` ở mức `warn`: Khoảng trống thời gian giữa các bản ghi telemetry (data gap) là hiện tượng vật lý thông thường do mạng truyền dẫn không dây chập chờn, xe đi vào hầm hoặc cảm biến bị trễ nhịp. Pipeline chỉ gắn cảnh báo (warn) để kỹ sư nhận biết tình trạng kết nối mà không dừng pipeline, vì các đoạn dữ liệu xung quanh vẫn có giá trị sử dụng.
  - `in_range` ở mức `error`: Giá trị tốc độ vượt khỏi giới hạn vật lý (âm hoặc lớn hơn 70 m/s ≈ 252 km/h) là dữ liệu rác nghiêm trọng do lỗi đọc phần cứng. Nếu cho phép dữ liệu vô lý này lọt vào Gold, mô hình xe tự lái sẽ học sai lệch hoàn toàn hàm mục tiêu (loss function), gây tai nạn chết người trong thực tế. Vì vậy, vi phạm luật này bắt buộc phải dừng pipeline lập tức (`error`).
- **Kết quả thí nghiệm `break-gate` (`break_gate.json`):**
  - Khi nới lỏng luật làm sạch lên `max_speed_mps = 1000`: Cổng chất lượng dbt test đã phát hiện và chặn lại thành công với kết quả: `PASS=5 WARN=1 ERROR=1 SKIP=0`.
  - Test bị vi phạm: `in_range_silver_telemetry_speed_mps__70__0` bắt chính xác **6 dòng** bất thường (3 dòng thuộc Drive A và 3 dòng thuộc Drive B). Sau khi khôi phục luật gốc, dbt test trở lại trạng thái sạch: `PASS=6 WARN=1 ERROR=0 SKIP=0`.
- **Vai trò của "cổng chất lượng" (Data Quality Gate):**
  - Cổng chất lượng là tuyến phòng thủ tự động và độc lập (chạy sau bước biến đổi ELT) để ngăn chặn rác dữ liệu (garbage-in, garbage-out) lan truyền sang các tầng phân tích và mô hình học máy downstream.
  - Khi có sự cố như kỹ sư vô tình sửa sai luật làm sạch hoặc hệ thống nguồn bị schema drift, quality gate ngăn chặn sự cố âm thầm, bảo vệ toàn vẹn độ tin cậy của dataset Gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

- **Số liệu từ thí nghiệm `idempotency.json` (chạy lại partition `trip-00003`):**
  - Số dòng ban đầu: **1246 dòng** (dấu vân tay nội dung `fingerprint`: `6c7480dfde7e`).
  - Số dòng sau hai lần chạy lại: vẫn chính xác **1246 dòng** (dấu vân tay giữ nguyên `6c7480dfde7e`, `unchanged: true`).
  - Khi ghi theo phương thức `append` sai lầm: Sau 2 lần chạy lại, riêng trip này (vốn chỉ có 60 khung) bị cộng dồn thành **120 dòng** (nhân đôi dữ liệu).
- **Vì sao Delta `overwrite` theo partition an toàn hơn xoá rồi ghi lại:**
  - *Tính nguyên tử (ACID Transaction)*: Delta Lake thực hiện ghi dữ liệu mới vào các file Parquet mới trong khi vẫn giữ nguyên các file cũ. Khi toàn bộ tác vụ ghi hoàn tất, transaction mới được commit vào `_delta_log`. Toàn bộ quá trình là atomic (all-or-nothing).
  - *Xử lý lỗi giữa chừng an toàn tuyệt đối*: Nếu node mạng bị sập hoặc gặp ngoại lệ giữa chừng, transaction bị hủy bỏ và không commit; người đọc (readers) vẫn đọc snapshot cũ hợp lệ, partition không bao giờ bị mất trắng.
  - Ngược lại, nếu thực hiện "xoá rồi ghi lại" theo cách truyền thống (Delete-then-Insert), nếu hệ thống chết ngay sau bước Delete thì partition sẽ biến mất hoàn toàn, gây thảm họa mất mát dữ liệu và mất tính nhất quán (data inconsistency).

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

- **Khung hình đen vs Khung hình tối:**
  - *Khung đen (`is_black`)*: Do lỗi che camera, hỏng cảm biến hoặc giải mã video lỗi (pixel variance gần như bằng 0). Khung đen hoàn toàn không mang thông tin thị giác nên pipeline loại bỏ triệt để khỏi Gold.
  - *Khung tối (`is_low_light`)*: Là khung hình ghi nhận trong điều kiện môi trường thực tế (xe đi qua đường hầm, ban đêm, dưới bóng râm). Khung tối vẫn chứa thông tin cấu trúc đường sá, đèn xe và vật thể. Pipeline gắn nhãn phân loại nhưng **giữ lại** (`dark_kept`: Drive A có 4 khung, Drive B có 61 khung).
  - *Quan điểm*: Hoàn toàn đồng ý. Xe tự lái bắt buộc phải hoạt động an toàn trong điều kiện thiếu sáng ban đêm. Nếu loại bỏ khung tối, mô hình sẽ bị thiên kiến (bias) và không bao giờ học được cách lái xe trong điều kiện trời tối.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- **Lựa chọn ngưỡng và sự phụ thuộc vào tốc độ:**
  - *Ngưỡng được chọn*: Ngưỡng **2** (giá trị mặc định tối ưu của pipeline).
  - *Lý do*: Ngưỡng 0 quá khắt khe, chỉ bắt các khung trùng từng bit và bỏ sót các khung gần như đứng yên nhưng có nhiễu cảm biến (sensor noise) như đoạn camera bị đơ 6 giây ở Drive A. Trong khi ngưỡng 6 quá lỏng, loại tới 363/602 khung ở Drive A (> 60%), làm mất đi các khung hình có chuyển động bình thường.
  - *Phụ thuộc vào tốc độ*: Khi xe chạy nhanh, cảnh vật thay đổi liên tục giữa 2 khung hình liên tiếp (khoảng cách Hamming lớn). Khi xe dừng đèn đỏ hoặc bò chậm, cảnh vật gần như đứng yên (khoảng cách Hamming rất nhỏ). Ngưỡng dHash cần được điều chỉnh theo dải tốc độ: khi xe đứng yên thì cho phép bắt trùng mạnh để giảm dư thừa, khi xe chạy nhanh thì hạ ngưỡng lọc để không làm mất dữ liệu huấn luyện quý giá.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

- **Số liệu từ `leakage.json` (so sánh 1019 khung hình thuộc 26 trip):**
  - *Chia theo khung hình (ngẫu nhiên 70/15/15)*: Có tới **26 trip** (100% tổng số trip) bị chia cắt trên nhiều split khác nhau (`per_frame_split_trips_straddling: 26`), và cả **26 trip** đều có khung hình xuất hiện đồng thời ở cả tập Train lẫn tập Test (`per_frame_split_trips_in_both_train_and_test: 26`).
  - *Chia theo trip (cách làm của pipeline)*: **0 trip** bị chia cắt (`per_trip_split_trips_straddling: 0`). Mỗi trip chỉ nằm trọn vẹn trong duy nhất 1 split.
- **Vì sao chia theo khung hình làm điểm kiểm thử đẹp giả tạo:**
  - Video xe tự lái là chuỗi thời gian liên tục (10 fps video gốc, mẫu 2 fps). Hai khung hình liền kề chỉ cách nhau 0.5 giây có độ tương đồng cực kỳ cao về khung cảnh con đường, góc camera, thời tiết, biển báo và xe cộ xung quanh.
  - Khi chia ngẫu nhiên theo khung hình, mô hình deep learning ở tập Train đã nhìn thấy gần như y hệt bối cảnh của khung hình ở tập Test (chỉ lệch vài phần trăm giây). Khi kiểm thử, mô hình đạt độ chính xác (accuracy) cực cao do chỉ việc "học vẹt/ghi nhớ" bối cảnh quen thuộc thay vì học khả năng tổng quát hóa (generalization).
  - Đây là lỗi rò rỉ dữ liệu (*Data Leakage*) kinh điển trong thị giác máy tính cho robotics: điểm benchmark trong phòng thí nghiệm rất đẹp nhưng khi đưa xe ra chạy trên một con đường mới hoàn toàn trong thực tế thì hệ thống sẽ nhận diện sai và gặp sự cố ngay lập tức.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- **Vì sao `dataset_version` là băm nội dung (content-based hashing) chứ không phải ngày giờ hay số đếm:**
  - *Tính tiền định và khả năng tái lập (Determinism & Reproducibility)*: Nếu mã nguồn, quy tắc lọc và nội dung dữ liệu đầu vào không thay đổi, mã băm sinh ra sẽ luôn giống hệt nhau. Nếu dùng ngày giờ (timestamp) hoặc số tự tăng (auto-increment), mỗi lần chạy lại pipeline (backfill, re-run) sẽ sinh ra một version mới dù dữ liệu bên trong không có gì khác biệt, gây lãng phí dung lượng lưu trữ và mất tính đồng bộ trong ML registry.
  - *Phát hiện thay đổi thực tế*: Bất kỳ một khung hình nào bị thay đổi nội dung, thêm bớt hoặc đổi nhãn đều dẫn đến mã băm bị thay đổi, giúp các hệ thống CI/CD nhận biết chính xác khi nào dataset thực sự bị drift.
- **Kết quả hai lần chạy liên tiếp:**
  - Cả hai lần chạy đều cho kết quả `dataset_version` giống hệt nhau: **`65ebc8820851`**.
- **Vì sao `dataset_version` của tôi khác bạn bè dù cùng dữ liệu gốc (liên hệ MSSV):**
  - Trong logic dbt model `gold_dataset_frames.sql`, thứ tự sắp xếp các trip để phân chia split train/val/test được tính bằng: `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`.
  - Biến môi trường `LAB_STUDENT_ID` nhận giá trị MSSV của tôi (`2A202602995`), đóng vai trò là chuỗi "salt". Do mỗi sinh viên có MSSV riêng biệt, hàm băm sẽ tạo ra thứ tự xếp hạng (rank) trip hoàn toàn khác nhau.
  - Điều này dẫn đến danh sách các trip được gán vào Train, Val, Test của mỗi người là khác nhau (dù cùng tuân theo tỷ lệ 70/15/15). Danh sách khung hình và manifest khác nhau dẫn đến mã băm `dataset_version` cuối cùng là duy nhất cho từng sinh viên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

- **Dataset này được tạo từ dữ liệu nào (trích xuất từ trường `lineage` trong `manifest.json`):**
  - *Hai video nguồn gốc (input videos)* với mã băm SHA-256 bất biến:
    - `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
    - `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
  - *Phiên bản commit của các bảng dữ liệu thượng nguồn (upstream table versions)* trên Delta Lake:
    - Bảng Bronze: `bronze.raw_telemetry_a: v0`, `bronze.raw_telemetry_b: v0`, `bronze.trips_raw: v0`, `bronze.video_files: v1`.
    - Bảng Silver: `silver.frames: v27` (tương ứng 26 commit partition của 26 trip + 1 commit khởi tạo), `silver.telemetry: v0`, `silver.frames_flagged: v0`, `silver.frame_telemetry: v0`.
  - *Các quy tắc lọc chất lượng*: Loại bỏ các khung hình thỏa mãn `is_black`, `is_blurry`, `is_near_dup`, hoặc `not sync_ok`.
- **Nếu 3 tháng sau cần tái tạo lại đúng dataset này:**
  1. Sử dụng tính năng **Time Travel** của Delta Lake (`VERSION AS OF` tương ứng trong bảng Delta) để đọc chính xác snapshot của các bảng Silver và Bronze tại thời điểm commit nêu trên, ngăn ngừa bất kỳ dữ liệu ghi đè nào sau này can thiệp vào.
  2. Dùng mã băm SHA-256 đối chiếu để đảm bảo 2 file video MP4 nguồn không bị chỉnh sửa hay ghi đè.
  3. Sử dụng đúng mã commit Git của project dbt (chứa các model SQL và ngưỡng lọc `sync_tol_ms`, `split_train=0.7`, `split_val=0.15`) cùng biến môi trường `LAB_STUDENT_ID=2A202602995` để đảm bảo thứ tự hash seed phân chia split hoàn toàn trùng khớp.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

- **Mô tả sự cố và quy trình phục hồi (trip của tôi là `trip-07304`):**
  - Khi đặt `LAB_FAIL_ONCE=trip-07304`, trong DAG `day09_video_pipeline`:
    - Đúng **1 task** chuyển sang màu ĐỎ (`failed`): task `silver_frames_trip-07304`. Mở log task thấy thông báo lỗi: `RuntimeError: simulated crash while extracting trip-07304; run it again`.
    - Có **7 task** phía sau chuyển sang màu cam `upstream_failed` (gồm: `dbt_silver_frames_flagged`, `dbt_silver_frame_telemetry`, `dbt_test_silver_frames`, `gold_dataset_frames`, `export_dataset_manifest`, `dbt_marts`, `publish_warehouse`).
  - *Thao tác khắc phục*: Trên giao diện Airflow Graph/Grid, nhấp vào task đỏ `silver_frames_trip-07304` và bấm **Clear** (chọn kèm downstream). Task được scheduler đưa vào hàng đợi và chạy lại. Do marker `.failed-trip-07304` đã được ghi nhận trong work dir, lần chạy thứ hai thành công và toàn bộ DAG chuyển sang màu XANH (all success).
  - *Tính toàn vẹn dữ liệu*: Nhờ cơ chế `overwrite` theo partition của Delta Lake, dữ liệu không hề bị nhân đôi (vẫn chính xác 1246 frames ở Silver và 1019 frames ở Gold).
  - *Tính tiền định của phiên bản*: `dataset_version` vẫn giữ nguyên là **`65ebc8820851`**. Chạy lại lệnh verify vẫn đạt trọn vẹn **27/27 checks passed**.
  - *Bằng chứng*: Ảnh chụp log task đỏ `report/screenshots/airflow_task_failed.png` (thấy rõ `trip-07304`), ảnh DAG Graph xanh `report/screenshots/airflow_graph.png`, và file kết quả `report/verify.txt`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **DuckDB chạy in-process trên single-node container** (Slide 22, 23):
   - *Nguy cơ*: DuckDB chỉ chạy trên 1 máy đơn lẻ, bị giới hạn bởi RAM và CPU vật lý. Khi nâng lên 500 xe/ngày (~hàng triệu khung hình và hàng chục triệu bản ghi telemetry), các thao tác join và dbt run sẽ gây tràn bộ nhớ (Out-Of-Memory - OOM) và làm crash container.
   - *Giải pháp thay thế*: Chuyển sang **Apache Spark (hoặc Databricks / Trino)** chạy trên cụm cluster phân tán, hỗ trợ tính toán song song quy mô lớn trên các bảng Delta Lake.
2. **Xử lý video bằng FFmpeg & LocalExecutor của Airflow trên một máy** (Slide 12, 24, 26):
   - *Nguy cơ*: Giải mã video MP4 và tính toán perceptual hash (dHash) bằng CPU ngốn tài nguyên cực lớn. 500 video mỗi ngày sẽ làm tắc nghẽn hàng đợi (bottleneck), khiến độ trễ xử lý (processing latency) kéo dài nhiều ngày.
   - *Giải pháp thay thế*: Sử dụng **Airflow KubernetesExecutor / CeleryExecutor kết hợp Ray hoặc AWS Batch / K8s Jobs hỗ trợ tăng tốc GPU**, phân tán tác vụ extract video ra hàng trăm worker chạy song song.
3. **PostgreSQL làm Data Warehouse phục vụ báo cáo BI** (Slide 16):
   - *Nguy cơ*: PostgreSQL là cơ sở dữ liệu quan hệ lưu trữ theo hàng (row-oriented), chỉ tối ưu cho các giao dịch OLTP nhỏ. Khi bảng gallery và telemetry tích lũy hàng chục triệu dòng, các truy vấn phân tích aggregation phức tạp từ Metabase sẽ bị chậm, nghẽn I/O và timeout.
   - *Giải pháp thay thế*: Thay thế bằng Data Warehouse dạng cột (columnar) chuyên dụng cho OLAP như **ClickHouse, Snowflake, hoặc BigQuery**, cho phép nén dữ liệu cao và phản hồi truy vấn phân tích chỉ trong vài mili-giây.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    trip_id,
    drive_id,
    total_frames,
    black_frames,
    blurry_frames,
    near_dup_frames,
    ROUND((black_frames + blurry_frames + near_dup_frames) * 100.0 / total_frames, 2) AS pct_bad_frames
FROM marts.mart_trip_summary
ORDER BY pct_bad_frames DESC
LIMIT 5;
```

- **Điều câu truy vấn này cho biết:**
  - Truy vấn thống kê Top 5 chuyến đi (trip) có tỷ lệ khung hình bị loại bỏ cao nhất do chất lượng kém (tổng khung đen, mờ, đứng hình/trùng lặp chia cho tổng số khung của trip).
  - Về mặt vận hành, chỉ số này giúp đội ngũ kỹ sư nhanh chóng phát hiện các xe hoặc cảm biến camera có vấn đề phần cứng (ví dụ camera bị rung lắc gây mờ liên tục, hoặc xe bị kẹt xe đứng yên quá lâu trong cảnh) để kịp thời lên lịch kiểm tra và bảo trì thiết bị trên xe.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất:** Tác động to lớn của việc chia split dữ liệu theo trip so với chia theo khung hình: chia ngẫu nhiên theo khung hình làm rò rỉ 100% trip giữa Train và Test, khiến model học vẹt mà ta không hề hay biết nếu không kiểm tra leakage.
- **Điều còn chưa rõ:** Cách xác định ngưỡng dung sai thời gian `sync_tol_ms` một cách tự động khi xe chạy qua nhiều địa hình có tần số lấy mẫu telemetry dao động bất thường mà không cần cấu hình tĩnh.
- **Lỗi dữ liệu pipeline chưa bắt được:** Hiện tượng giọt nước mưa, bùn đất bắn lên che một góc ống kính (camera partial occlusion) hoặc đèn pha xe đối diện chiếu thẳng vào gây lóa sáng cục bộ (lens flare). Các khung hình này không bị coi là đen, mờ hay trùng nhưng đã bị mất thông tin làn đường quan trọng.
