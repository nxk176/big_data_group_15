# Hướng dẫn chạy và giải thích demo checkpoint HarborTrace

Tài liệu này đi cùng `scripts/week04_checkpoint.py` trong gói checkpoint tuần 4. Các kết quả chi tiết dưới đây đã được chạy với `--details` ngày 22/10/2026. Người lấy repo về có thể làm theo phần “Run the checkpoint” trong [README](../README.md) để thiết lập môi trường và chạy demo.

Kiểm tra phát hành ngày 25/10/2026: xuất đúng các tệp staged của Git sang thư mục sạch và chạy lại bằng môi trường WSL đã có. Demo trả về `PASS`, exit code 0; thư mục kiểm tra không chứa các module hay dữ liệu đầy đủ chưa công bố.

Nếu chưa hiểu project, bắt đầu từ mục 0 rồi đọc mục 1–5 và chạy demo một lần. Mục 6–9 giải thích Spark, nguồn dữ liệu, kiến trúc và các câu thường bị hỏi. Mục 10–12 giúp bạn luyện nói, mở đúng mã nguồn và demo trong 5 phút. Cuối cùng dùng mục 13 để tự kiểm tra xem mình đã hiểu chưa.

**0. Hiểu project từ đầu: nhóm đang làm việc gì?**

HarborTrace lấy những bản ghi vị trí rời rạc của tàu và ghép chúng thành các đợt hiện diện quan sát được trong vùng cảng Los Angeles và Long Beach. Nói đơn giản: từ nhiều dòng “tàu nào, lúc nào, ở tọa độ nào”, nhóm tạo ra bản ghi “tàu này được quan sát trong vùng cảng từ thời điểm nào đến thời điểm nào, có đủ bằng chứng xác nhận chưa, còn thiếu thông tin gì”.

Một tàu có thể phát nhiều bản ghi trong cùng một đợt ở cảng. Nếu chỉ đếm số dòng thì tàu phát tín hiệu thường xuyên sẽ có số đếm cao hơn, dù số đợt hiện diện không nhiều hơn. Vì vậy phải xử lý theo từng tàu và theo thời gian để gom quan sát thành visit. Dữ liệu còn có thể đến sai thứ tự, trùng, mất tín hiệu hoặc dao động gần ranh giới.

Các visit sau đó có thể dùng để thống kê số đợt hiện diện theo cảng/giờ và phân bố khoảng thời gian quan sát. Mỗi kết quả cần giữ chất lượng và bằng chứng nguồn để người xem biết nên tin đến mức nào. Để kết luận về cập bến thương mại, bốc dỡ hàng hoặc tắc nghẽn thực sự, cần thêm dữ liệu xác minh độc lập.

| Câu cần trả lời | Nội dung của project |
|---|---|
| Đầu vào là gì? | Vị trí AIS lịch sử của NOAA và polygon vùng cảng của USACE. |
| Nhóm biến đổi chúng thế nào? | Kiểm tra/chuẩn hóa dữ liệu, gán điểm vào vùng cảng, gom theo tàu và thời gian rồi áp dụng quy tắc visit. |
| Đầu ra là gì? | Visit kèm thời gian quan sát, trạng thái, cờ bất định và liên kết đến các dòng hỗ trợ; về sau có thống kê và dashboard. |
| Checkpoint này chạy phần nào? | Spark batch nhận các quan sát đã có assignment, tái dựng visit và so với đáp án kiểm thử. |
| Vì sao phải dùng hệ thống Big Data? | Nguồn thực có hàng triệu dòng mỗi ngày; cần cách lưu trữ, xử lý theo khóa, phát lại và đối soát có thể mở rộng. Demo nhỏ dùng để kiểm tra logic trước khi đánh giá trên tập lớn. |

Ví dụ đầu vào sau khi đã xác định trong/ngoài vùng cảng:

| Tàu giả lập | Thời điểm UTC | Có ở trong vùng cảng không? |
|---|---|---|
| A | 00:00 | Ngoài |
| A | 00:05 | Trong |
| A | 00:15 | Trong |
| A | 00:20 | Ngoài |
| A | 00:30 | Ngoài |

Năm dòng trên tạo một visit. Đầu ra chính có thể đọc thành: “Tàu A có hai quan sát trong vùng lúc 00:05 và 00:15; đợt này đạt quy tắc xác nhận 10 phút; có bằng chứng ngoài ở hai phía để giới hạn khoảng vào và ra.” Cách tính chi tiết ở mục 3–4.

Các từ sẽ gặp trong mã và khi trình bày:

| Từ | Cách hiểu trong project |
|---|---|
| AIS | Nguồn các bản ghi nhận dạng/vị trí tàu; nhóm dùng bản lịch sử đã được NOAA xử lý. |
| MMSI | Mã định danh hàng hải dùng để gom bản ghi; không mặc định là căn cước tàu hoàn hảo trong mọi thời kỳ. |
| Polygon | Một vùng hình học trên bản đồ biểu diễn ranh giới cảng đã chọn. |
| `assignment` | Kết quả gán quan sát vào cảng; `4120` là Los Angeles, `4110` là Long Beach. Trong fixture, `null` biểu diễn ngoài cảng đang kiểm thử. Dữ liệu thật còn có thể có trường hợp mơ hồ và phải đọc thêm flags. |
| Canonical | Dạng quan sát được chuẩn hóa để các phần xử lý dùng chung: thời gian UTC, tọa độ, ID, assignment và cờ chất lượng. |
| Observation | Một quan sát vị trí ở một thời điểm. |
| Visit | Một đợt hiện diện được tái dựng từ chuỗi quan sát theo quy tắc. |
| Fixture | Bộ dữ liệu kiểm thử có chủ đích và có đáp án kỳ vọng. |
| Batch | Xử lý một tập dữ liệu đã có, ví dụ dữ liệu lịch sử một ngày. |
| Streaming | Xử lý các sự kiện khi chúng đi vào hệ thống; trong kiến trúc này thử nghiệm bằng replay lịch sử. |
| State | Những thông tin thuật toán phải nhớ khi đọc quan sát tiếp theo, ví dụ đã vào hay đang chờ xác nhận ra. |
| Partition | Một phần dữ liệu Spark phân công cho task xử lý; nó không đồng nghĩa với một máy vật lý. |

Sản phẩm cuối cùng được thiết kế gồm hai đường xử lý:

```mermaid
flowchart LR
  N[NOAA: vị trí tàu] --> C[Chuẩn bị quan sát canonical]
  U[USACE: vùng cảng] --> C
  C --> H[HDFS: dữ liệu lưu lại]
  H --> B[Spark batch: tái dựng lịch sử]
  C --> K[Kafka: phát lại sự kiện]
  K --> S[Spark streaming: kết quả tạm thời]
  B --> M[MongoDB: kết quả phục vụ truy vấn]
  S --> M
  M --> D[Dashboard: xem visit và thống kê]
```

Đường batch giúp tính lại trên lịch sử đầy đủ của phạm vi chọn. Đường streaming giúp quan sát kết quả tạm thời trong lúc sự kiện đến. HDFS giữ dữ liệu để đọc lại; Kafka chuyển sự kiện; MongoDB phục vụ truy vấn cho giao diện. Bản thiết kế chi tiết có bước kiểm tra generation trước khi đổi kết quả đang phục vụ ở [sơ đồ kiến trúc](week03_architecture.md).

Hãy phân biệt ba mức bằng chứng khi nói trước lớp:

| Mức | Có gì? | Có thể chứng minh bằng gì? |
|---|---|---|
| GitHub checkpoint tuần 4 | Spark batch, fixture, excerpt NOAA, hợp đồng/rule và tài liệu kiến trúc. | Clone repo, cài hai thư viện đã ghim phiên bản và chạy lệnh ở README. |
| Nguyên mẫu cục bộ đầy đủ hơn | Nguồn NOAA pilot, canonical và các kết quả/manifest đã chạy trước đó; có thêm mã chưa đưa vào commit checkpoint. | Mở file và manifest trong `E:\big_data\data`; các con số pilot ở mục 7. |
| Mục tiêu toàn môn | Toàn bộ Lambda pipeline, đánh giá và bằng chứng triển khai đáp ứng rubric cuối môn. | Phải đưa ra mã, thí nghiệm và môi trường triển khai tương ứng ở các mốc sau. |

Lệnh tuần 4 không tải nguồn lớn, không tự chạy phép gán tọa độ vào polygon, không khởi động các dịch vụ của toàn hệ thống. Nó kiểm chứng một bước nghiệp vụ cụ thể bằng chính adapter Spark được dùng cho batch của nguyên mẫu.

**1. Chạy trên laptop hiện tại**

Mở PowerShell và chạy nguyên lệnh sau:

```powershell
wsl.exe -- bash -lc 'cd /mnt/e/big_data && source ~/.venvs/harbortrace/bin/activate && PYTHONPATH=src python scripts/week04_checkpoint.py --details'
```

Hoặc gõ `wsl` để vào Ubuntu rồi chạy:

```bash
cd /mnt/e/big_data
source ~/.venvs/harbortrace/bin/activate
PYTHONPATH=src python scripts/week04_checkpoint.py --details
```

`/mnt/e/big_data` là cùng thư mục với `E:\big_data`. `source` kích hoạt môi trường Python đã cài thư viện của dự án. `PYTHONPATH=src` giúp cả driver và Python worker tìm được package `harbortrace` trong mã nguồn. `--details` in từng kết quả và kế hoạch thực thi. Bỏ tùy chọn này nếu chỉ cần dòng tổng kết.

Lệnh đã chạy thành công trong khoảng 18 giây ở lần kiểm tra gần nhất; thời gian trên máy có thể thay đổi. Demo chạy terminal và tự dừng Spark khi hoàn tất. Nó không mở dashboard.

Nếu chạy trên Ubuntu/WSL mới, cần Python 3.12, Java 17 và thư viện trong `requirements-week04.txt`. Đứng tại thư mục gốc của repo vừa clone rồi chạy:

```bash
python3 -m venv ~/.venvs/harbortrace-week04
source ~/.venvs/harbortrace-week04/bin/activate
python -m pip install -r requirements-week04.txt
PYTHONPATH=src python scripts/week04_checkpoint.py --details
```

**2. Đọc dòng tổng kết**

```json
{
  "fixture_cases": 5,
  "input_observations": 23,
  "real_excerpt_observations": 2,
  "real_excerpt_visits": 1,
  "result": "PASS",
  "sort_in_physical_plan": true,
  "spark_version": "3.5.7",
  "status_counts": {"candidate": 1, "reconciled": 5},
  "visits": 6
}
```

| Trường | Ý nghĩa chính xác |
|---|---|
| `fixture_cases: 5` | Có 5 tình huống kiểm thử đã thiết kế trước, với kết quả kỳ vọng trong JSON. |
| `input_observations: 23` | Tổng 23 quan sát của bộ kiểm thử tổng hợp; chưa bao gồm 2 dòng NOAA thật. |
| `visits: 6` | Bộ tổng hợp tạo 6 bản ghi visit. Một tình huống bị tách thành hai visit nên số visit lớn hơn số tình huống. |
| `reconciled: 5` | Có 5 visit tổng hợp đạt quy tắc xác nhận quan sát trong vùng cảng. Đây là nhãn của kết quả batch. |
| `candidate: 1` | Có 1 visit tổng hợp chưa đủ bằng chứng thời gian để xác nhận. |
| `real_excerpt_observations: 2` | Số quan sát đọc được từ trích đoạn NOAA; mã kiểm tra thực sự đếm và đối chiếu số này. |
| `real_excerpt_visits: 1` | Hai quan sát thật tạo một visit riêng. Nó không nằm trong `visits: 6` hay `status_counts` của bộ tổng hợp. |
| `sort_in_physical_plan: true` | Kế hoạch thực thi của DataFrame tổng hợp chứa bước `Sort`. |
| `result: PASS` | Các số visit, thuộc tính được ghi trong đáp án, điều kiện sort và kết quả trích đoạn thật đều đạt kiểm tra. Không phải tỷ lệ chính xác 100% trên thế giới thực. |

**3. Quy tắc tạo visit và lý do cần giữ trạng thái**

Một quan sát là một lần ghi nhận vị trí của một MMSI tại một thời điểm. Một visit là một nhóm quan sát được thuật toán liên kết thành một đợt hiện diện trong vùng cảng.

- Gặp điểm trong vùng cảng: bắt đầu candidate.
- Có các quan sát trong vùng cách nhau ít nhất 10 phút trong cùng đợt theo quy tắc: xác nhận visit.
- Gặp điểm ngoài vùng: bắt đầu chờ xác nhận ra. Hai điểm ngoài cách nhau ít nhất 10 phút xác nhận ra.
- Tàu quay lại trong trước khi đủ điều kiện ra: giữ visit và gắn cờ `short_excursion_or_boundary_jitter`.
- Khoảng cách giữa hai quan sát liên tiếp lớn hơn 30 phút: kết thúc đợt cũ vì không biết tàu làm gì trong khoảng mất tín hiệu; bắt đầu lại từ quan sát hiện tại.
- Hết dữ liệu mà chưa xác nhận ra: kết thúc visit với cờ `right_censored`.

Các ngưỡng 10/10/30 phút là tham số thiết kế `visits-v1`. Cần đánh giá độ nhạy ở các giá trị khác; chưa có căn cứ để gọi chúng là tiêu chuẩn hàng hải. Chính xác 30 phút không vượt điều kiện `> 30` của quy tắc gap.

State machine lưu lần vào đầu/cuối, lần ngoài trước đó, thời điểm bắt đầu chờ ra, số quan sát và các cờ. Nếu chỉ kiểm tra từng điểm độc lập thì không thể quyết định đợt hiện diện đã đủ thời gian hay chưa, hoặc điểm ngoài có phải là một lần ra thực sự hay chỉ là dao động gần ranh giới.

**4. Năm tình huống và kết quả thực tế**

| Tình huống | Quan sát đáng chú ý | Kết quả |
|---|---|---|
| `normal_entry_exit` | Ngoài 00:00; trong 00:05, 00:15; ngoài 00:20, 00:30 | Một reconciled visit, observed span 600 giây; có bằng chứng chặn hai đầu. |
| `crosses_midnight` | Trong từ 23:55 đến 00:05 hôm sau | Một reconciled visit, span 600 giây; đổi ngày không tự tách visit. |
| `long_gap_splits` | Trong 00:05, 00:15; điểm tiếp theo trong lúc 01:00 | Gap 45 phút tách thành hai visit: đợt đầu reconciled, đợt sau chỉ có một quan sát nên candidate. |
| `input_range_censoring` | Chỉ có hai điểm trong lúc 00:00 và 00:10 | Đạt xác nhận, nhưng thiếu bằng chứng trước lúc vào và sau lúc ra nên censored cả hai đầu. |
| `short_excursion` | Trong 00:05, 00:15; ngoài 00:20; trong lại 00:25; ngoài 00:30, 00:40 | Một reconciled visit, span 1.200 giây; gắn cờ excursion/jitter vì lần ngoài đầu chưa đủ điều kiện xác nhận ra. |

Ví dụ normal entry/exit:

```text
00:00 ngoài → 00:05 trong → 00:15 trong → 00:20 ngoài → 00:30 ngoài
```

Giờ vào có thể nằm giữa 00:00 và 00:05; giờ ra có thể nằm giữa 00:15 và 00:20. Chương trình biết các khoảng từ các điểm quan sát, chưa biết giờ vào/ra chính xác. `observed_span_seconds = last_inside - first_inside = 600` là khoảng thời gian giữa hai quan sát trong đầu và cuối. Nó không chứng minh tàu liên tục ở trong cảng suốt 600 giây.

**5. Giải thích visit NOAA thật**

Hai quan sát của MMSI `367721190` được ghi nhận lúc `02:35:34Z` và `02:53:34Z` ngày 01/01/2024, cùng assignment `4120` (Los Angeles). Hai thời điểm cách nhau 18 phút, tức 1.080 giây: đủ ngưỡng xác nhận 10 phút và chưa vượt gap 30 phút.

| Thuộc tính | Giải thích |
|---|---|
| `observation_count: 2` | Hai quan sát trong vùng hỗ trợ visit. |
| `observed_span_seconds: 1080` | Khoảng cách thời gian giữa hai lần ghi nhận. |
| `status: reconciled` | Đạt quy tắc xác nhận batch. |
| `left_censored: true` | Thiếu điểm ngoài trước quan sát trong đầu tiên; chưa có mốc chặn dưới cho giờ vào. |
| `right_censored: true` | Chưa có cặp điểm ngoài xác nhận ra; chưa có mốc chặn trên cho giờ ra. |
| `possible_entry_after: null` | Chưa biết thời điểm ngoài trước lần vào. |
| `possible_entry_by` | Quan sát trong đầu tiên: theo mô hình, tàu đã có mặt trong vùng ở thời điểm này. |
| `possible_exit_after` | Quan sát trong cuối cùng. |
| `possible_exit_by: null` | Thiếu bằng chứng để chặn giờ ra. |
| `boundary_topology_repaired` | Assignment sử dụng hình học Los Angeles đã được sửa topology; cần giữ cờ bất định. |
| `input_range_ended` | Dữ liệu đầu vào đã hết khi visit còn mở. |
| `source_record_ids`, `source_digest` | Liên kết kết quả với các dòng dữ liệu hỗ trợ; hỗ trợ truy vết và kiểm tra tính lặp lại. |

`reconciled` và `censored` đo hai việc khác nhau: visit có thể đủ điều kiện xác nhận hiện diện, nhưng vẫn thiếu dữ liệu để xác định khoảng vào/ra. Tàu không nhất thiết đã rời cảng khi file kết thúc.

**6. Spark thực sự làm gì trong demo**

```mermaid
flowchart LR
  A[Quan sát canonical] --> B[DataFrame]
  B --> C[Repartition theo MMSI]
  C --> D[Sort trong partition]
  D --> E[mapPartitions: state machine]
  E --> F[DataFrame visits]
  F --> G[Đối chiếu đáp án]
```

`repartition(2, "mmsi")` phân phối lại dữ liệu theo hash của MMSI. Tất cả quan sát của cùng một MMSI đi vào cùng partition; mỗi partition có thể chứa nhiều MMSI. Đây là shuffle, có chi phí di chuyển dữ liệu. Chỉ repartition chưa đảm bảo thứ tự thời gian.

`sortWithinPartitions("mmsi", "event_time", "logical_id", "source_record_id")` đưa các dòng về thứ tự để xử lý lần lượt. Các thời điểm canonical trong demo đều là UTC ở định dạng ISO thống nhất. Stable IDs làm thứ tự rõ ràng khi trùng thời điểm. Trong physical plan, `Exchange hashpartitioning(mmsi, 2)` là phần phân phối lại, còn `Sort [...]` là phần sắp xếp. Đây là sort trong từng partition, không phải global sort của toàn bộ tập.

`mapPartitions` chạy state machine theo iterator của từng partition. Khi đổi MMSI, nó đóng trạng thái tàu cũ và khởi tạo tàu mới. Không cần đưa toàn bộ quan sát của mọi tàu về driver. Mã dùng RDD `mapPartitions` cho logic có trạng thái theo chuỗi; đây không phải SQL UDF. Trade-off là chi phí Python/JVM và ít khả năng để Catalyst tối ưu bên trong hàm Python.

`local[2]` chạy Spark trên laptop với tối đa hai task đồng thời, không tương đương hai máy worker. `collect()` trong demo lấy kết quả rất nhỏ về driver để kiểm tra. Với dữ liệu lớn, các kết quả phải được ghi qua storage thay vì collect toàn bộ lên driver. Hai quan sát thật kiểm tra tính đúng của đường chạy, không đo khả năng mở rộng.

Tài liệu chính thức: [sortWithinPartitions](https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sortWithinPartitions.html) và [Spark submission/master settings](https://spark.apache.org/docs/3.5.7/submitting-applications.html).

**7. Dữ liệu lớn của dự án ở đâu**

Dữ liệu NOAA 2024 là lịch sử đã qua xử lý của nhà cung cấp. AIS cung cấp quan sát vị trí; MMSI là định danh radio hàng hải 9 chữ số và có hạn chế về nhận dạng tàu theo thời gian. Snapshot USACE đang dùng là **Ports layer ID 0**; layer PortStatisticalAreas ID 1 là một lớp khác.

Pilot ngày 01/01/2024 đã có cục bộ:

- `data/source/ais-2024-01-01.parquet`: 227.181.435 byte, 7.293.408 dòng toàn nguồn.
- `data/canonical/ais-2024-01-01-regional.parquet`: 235.713 quan sát vùng LA/LB, giữ cả các điểm ngoài polygon nhưng nằm trong vùng chọn để hỗ trợ suy luận ra/vào.
- Đối soát: `235.713 canonical + 7 duplicate + 7.057.688 ngoài vùng = 7.293.408 dòng nguồn`.
- Kết quả batch pilot đã đo: 370 visit, gồm 271 reconciled và 99 candidate. Đây là bằng chứng từ lần chạy nguyên mẫu đầy đủ; lệnh checkpoint không tạo lại các con số này.
- Hai dòng NOAA thật trong Git là trích đoạn từ canonical; dữ liệu tổng hợp dùng để kiểm thử các tình huống có chủ đích.

Canonical nghĩa là biểu diễn chuẩn hóa có timestamp UTC, tọa độ, định danh dòng nguồn, logical ID, assignment và quality flags. Demo nhận assignment đã chuẩn bị sẵn; không tải lại NOAA, không chạy lại phép kiểm tra point-in-polygon. Trong nguyên mẫu cục bộ, phần chuẩn bị nguồn dùng PyArrow/SQLite và Shapely; Spark xử lý phần tái dựng visit và các phân tích batch. SHA-256 giúp kiểm tra đúng phiên bản byte của file, không chứng minh vị trí hoặc kết luận nghiệp vụ là đúng.

Trong fixture tổng hợp, tọa độ được dùng làm giá trị giữ chỗ; `assignment` của từng sự kiện được đặt sẵn theo kịch bản. Vì vậy hai sự kiện có thể cùng tọa độ giữ chỗ nhưng khác assignment. Điều này có chủ đích để kiểm tra state machine độc lập với hình học. Trích đoạn NOAA thật giữ tọa độ, assignment, ID và quality flags lấy từ canonical. Demo này chưa kiểm thử lại tính đúng của phép point-in-polygon.

Các URL và checksum đầy đủ ở [ghi chú checkpoint](week04_checkpoint.md). Bản ghi manifest nằm cạnh dữ liệu nguồn và canonical. Có thể mở các file manifest này để trình bày provenance mà không tải file lớn lên GitHub.

**8. Nối demo với kiến trúc Lambda**

Mở [sơ đồ tuần 3](week03_architecture.md): batch tái dựng lịch sử từ dữ liệu chuẩn hóa; speed path nhận replay lịch sử qua Kafka và tạo kết quả provisional. Sau đó batch là cơ sở đối soát/cập nhật kết quả khi có dữ liệu muộn. HDFS lưu nguồn và các lớp Parquet; MongoDB phục vụ kết quả theo generation; dashboard đọc kết quả.

Lệnh checkpoint chỉ minh họa nhánh Spark batch với dữ liệu nhỏ từ filesystem. HDFS có smoke test riêng của tuần 2; Kafka, MongoDB và dashboard không được khởi động bởi lệnh này. Các phần nguyên mẫu cục bộ và kế hoạch kiến trúc cần được phân biệt với những gì đã công bố trên Git.

**9. Các câu thường bị hỏi**

| Câu hỏi | Cách trả lời dựa trên bằng chứng |
|---|---|
| Hai dòng dữ liệu thì Big Data ở đâu? | Đây là mẫu nhỏ để chạy lại và kiểm thử nhanh. Quy mô nguồn thực và row accounting đã có ở pilot; Spark partition theo tàu để áp dụng cùng logic trên dữ liệu lớn. Mẫu nhỏ không dùng làm benchmark scalability. |
| Sao không đếm điểm trong polygon là xong? | Một tàu phát nhiều quan sát. Đếm điểm sẽ đếm tần suất phát và độ phủ. Cần gom theo tàu/thời gian để tạo đợt hiện diện, xử lý gap, excursion và thiếu dữ liệu hai đầu. |
| Có chắc đây là một lần cập cảng không? | Đây là observed presence theo polygon và rule version. Chưa có dữ liệu berth/call độc lập để xác nhận cập bến thương mại. |
| Dữ liệu có trực tiếp không? | Nguồn là lịch sử NOAA. Kafka trong kiến trúc phát lại lịch sử có kiểm soát để thử duplicate/delay; không phải bộ thu AIS trực tiếp. |
| `PASS` chứng minh gì? | Đường Spark tạo số visit và các thuộc tính được kiểm tra đúng trên fixture và excerpt; chưa đo độ chính xác toàn tập, hiệu năng hay fault tolerance. |
| Tại sao không dùng pandas? | pandas đủ cho fixture. Spark phục vụ pipeline dữ liệu lớn hơn, phân phối theo khóa và có execution plan. Lựa chọn này cần được hỗ trợ bằng benchmark thực ở các mốc sau. |
| Vì sao sort quan trọng? | State machine xử lý chuỗi sự kiện. Nhầm thứ tự có thể đổi giờ vào, gap hoặc việc tách/gộp visit. Repartition chỉ gom khóa, không tự sort. |
| Nếu một tàu có rất nhiều bản ghi? | Nó có thể làm lệch tải một partition. Cần đo skew và tài nguyên; không thể chia ngẫu nhiên một tàu cho nhiều partition khi chưa thiết kế cách ghép trạng thái ở ranh giới. |
| Có exactly-once chưa? | Lệnh batch này không kiểm thử cam kết streaming đó. Duplicate identity, checkpoint và sink idempotency phải được kiểm chứng ở đường streaming/sink. |
| Tại sao có candidate? | Giữ lại dấu vết quan sát ít thay vì xóa hoặc gọi là confirmed. Tổng hợp hoạt động mặc định loại candidate. |
| Tại sao có cờ sửa polygon? | Hình học nguồn Los Angeles không hợp lệ; bản gốc được giữ lại, bản sửa dùng tính toán có cờ. Việc sửa có thể đổi assignment và cần được đánh giá. |
| Có đáp ứng toàn bộ môn chưa? | Checkpoint có code chạy được, dữ liệu nguồn/mẫu và kiến trúc. Còn cần công bố dần các tính năng Spark, streaming, đánh giá, kiểm thử hệ thống và bằng chứng triển khai Kubernetes/cloud theo yêu cầu cuối môn. |

**10. Trình bày ngắn trước khi chạy**

“HarborTrace tái dựng các đợt hiện diện quan sát được của tàu trong vùng cảng LA/LB từ AIS lịch sử. Nhóm đã chuẩn bị nguồn NOAA và polygon USACE, thiết kế Lambda và kiểm tra HDFS. Demo này chạy phần Spark batch: gom theo MMSI, sắp xếp theo thời gian rồi dùng state machine tạo visit. Nhóm dùng năm fixture có đáp án để kiểm tra các trường hợp thiếu dữ liệu và dao động ranh giới, đồng thời chạy một trích đoạn thật. Kết quả sẽ giữ các cờ bất định, thay vì suy ra giờ cập/rời bến chính xác từ dữ liệu thiếu.”

Nếu thấy `WARN NativeCodeLoader` hoặc cảnh báo hostname như lần chạy đã kiểm chứng, hãy đọc tiếp đến kết quả cuối. Nếu có `Traceback`, `AssertionError` hoặc không có `PASS`, cần giải quyết lỗi trước khi trình diễn. `ModuleNotFoundError: harbortrace` thường do thiếu `PYTHONPATH=src` hoặc chạy sai thư mục. Kiểm tra Java bằng `java -version` và Python bằng `python --version` trong WSL; các câu lệnh Linux trong tài liệu phải chạy trong WSL.

**11. Mở đúng file và nối mã với ý tưởng**

| Khi muốn giải thích | Mở file | Chỉ vào phần nào? |
|---|---|---|
| Kiến trúc hệ thống | [week03_architecture.md](week03_architecture.md) | Sơ đồ batch/speed path và vai trò HDFS/Kafka/MongoDB. |
| Dữ liệu kiểm thử và đáp án | [reference_cases.json](../tests/fixtures/reference_cases.json) | `events` là chuỗi đầu vào; `expected` ghi số visit và các thuộc tính cần khớp. |
| Dữ liệu NOAA thật dùng trong demo | [real_ais_excerpt.jsonl](../tests/fixtures/real_ais_excerpt.jsonl) | Hai dòng cùng MMSI, event time cách nhau 18 phút, assignment `4120`, cờ sửa polygon. |
| Chương trình chạy đầu tiên | [week04_checkpoint.py](../scripts/week04_checkpoint.py) | `main()` đọc fixture, tạo quan sát, cấu hình và gọi Spark; sau đó đối chiếu kết quả. |
| Spark chia và sắp dữ liệu | [spark_core.py](../src/harbortrace/spark_core.py) | `reconstruct_frame()` gọi `repartition`, `sortWithinPartitions`, rồi `mapPartitions`. |
| Một task tạo visit ra sao | [spark_core.py](../src/harbortrace/spark_core.py) | `_reconstruct_partition()` đọc iterator, tạo state machine theo MMSI và đóng khi đổi tàu. |
| Quy tắc nghiệp vụ | [visits.py](../src/harbortrace/visits.py) | `Rules` chứa các ngưỡng; `VisitMachine.feed()` cập nhật trạng thái khi đọc một quan sát. |
| Tạo bản ghi kết quả | [visits.py](../src/harbortrace/visits.py) | `_finish()` tạo `Visit`, gắn status/censoring; `close()` đóng đợt còn mở khi hết dữ liệu. |
| Các trường dữ liệu | [contracts.py](../src/harbortrace/contracts.py) | `Observation` là đầu vào, `Visit` là đầu ra; `digest()` tạo ID theo các giá trị. |
| Phụ thuộc để chạy | [requirements-week04.txt](../requirements-week04.txt) | Spark Python API `pyspark` và cầu nối Python/Java `py4j`, được ghim phiên bản. |

Đọc `main()` theo trình tự sau sẽ thấy kết quả được tính chứ không phải chỉ in ra một đáp án cố định:

1. Đọc 5 kịch bản từ JSON. Mỗi kịch bản có MMSI tổng hợp riêng để không trộn các tàu khi xử lý chung.
2. Tạo 23 `Observation`, gồm thời điểm UTC, source ID, logical ID và assignment. Cấu hình đặt Spark `local[2]`, 2 partition và rule 10/10/30 phút.
3. Tạo Spark DataFrame từ các quan sát tổng hợp. Với hai dòng thật, Spark đọc trực tiếp file JSONL.
4. Gọi `reconstruct_frame()` để chia theo MMSI, sort và chạy state machine trên từng partition.
5. Lấy tập kết quả nhỏ về driver, so số visit và các thuộc tính có trong `expected`. Nếu lệch, chương trình ném `AssertionError`.
6. Kiểm tra excerpt thật có đúng 2 dòng; kết quả phải là 1 visit, status reconciled, span 1.080 giây, censored hai đầu.
7. In chi tiết nếu có `--details`, in summary và dừng Spark trong `finally`.

Trong demo, driver tạo bộ tổng hợp nhỏ rồi dùng `parallelize`; đây là cách đưa fixture vào Spark. Với pipeline dữ liệu lớn, nguyên mẫu batch đọc canonical Parquet thay vì tạo toàn bộ quan sát trong bộ nhớ driver. Việc demo `collect()` một vài kết quả để so đáp án cũng khác với thu toàn bộ dữ liệu lớn về driver.

Nếu giảng viên hỏi “Đổi ngưỡng 10 phút thành 20 phút thì sao?”, bạn có thể suy luận: ví dụ normal chỉ có span 10 phút và excerpt thật có span 18 phút, nên chúng sẽ không đủ điều kiện xác nhận theo ngưỡng mới. Đáp án fixture hiện được viết cho `visits-v1` 10/10/30; thay rule mà giữ nguyên đáp án sẽ làm kiểm tra thất bại. Không cần sửa mã ngay trong buổi demo để giải thích điều này.

**12. Kịch bản demo 5 phút và câu nói đi kèm**

Chuẩn bị sẵn Markdown preview, terminal PowerShell, file fixture và `spark_core.py`. Xem [README công khai](../README.md) để đối chiếu lệnh của người clone repo mới. Trên laptop này dùng lệnh mục 1, không cần tạo lại môi trường đang có.

| Thời gian | Thao tác | Bạn giải thích |
|---|---|---|
| 0:00–0:45 | Mở sơ đồ kiến trúc. | “Nhóm lấy vị trí tàu rời rạc để tái dựng các đợt hiện diện trong cảng. Đây là thiết kế Lambda; phần trình diễn hôm nay là nhánh Spark batch.” |
| 0:45–1:20 | Mở fixture và excerpt NOAA. | “Fixture có đáp án để kiểm tra trường hợp bình thường, gap và thiếu dữ liệu. Excerpt là hai quan sát thật từ nguồn NOAA đã chuẩn hóa.” |
| 1:20–2:00 | Chạy lệnh `--details` ở mục 1. | “Spark gom cùng tàu về một partition, sort theo thời gian rồi lần lượt cập nhật trạng thái vào/ra.” |
| 2:00–3:00 | Chỉ vào `normal_entry_exit`, `long_gap_splits` và summary. | “23 quan sát của 5 kịch bản tạo 6 visit. Trường hợp gap dài bị tách thành hai đợt, nên số visit lớn hơn số kịch bản.” |
| 3:00–4:00 | Chỉ vào JSON visit thật. | “Hai điểm cách nhau 18 phút nên đạt xác nhận. Chưa có điểm ngoài ở hai phía nên chưa biết khoảng vào/ra đầy đủ; các cờ censored được giữ lại.” |
| 4:00–5:00 | Mở `reconstruct_frame()` và nói phần tiếp theo. | “Mã có bước shuffle và sort thật trong physical plan. Các mốc sau sẽ công bố tiếp ingestion, streaming, serving và đánh giá hệ thống theo kiến trúc.” |

Nếu giảng viên muốn xem dữ liệu lớn đã sẵn trên máy, có thể dùng PowerShell để kiểm tra file và row accounting của pilot, không phải tải lại dữ liệu:

```powershell
Get-Item E:\big_data\data\source\ais-2024-01-01.parquet |
  Select-Object Name, Length

$pilotManifest = Get-Content -Raw -Encoding utf8 E:\big_data\data\canonical\ais-2024-01-01-regional.manifest.json | ConvertFrom-Json
$pilotManifest.row_accounting | Format-List
```

Bạn giải thích: “Đây là snapshot cục bộ đã chuẩn bị. Các nhóm dòng phải cộng lại bằng số dòng nguồn. Lệnh checkpoint nhỏ không tái tạo lại toàn bộ pilot; nó giúp kiểm tra nhanh một phần logic trước khi trình diễn pipeline lớn hơn.” Nếu chạy trên máy mới chỉ có GitHub checkout thì các file pilot lớn này chưa có; dùng provenance đã ghi và mẫu đi kèm repo.

**13. Tự kiểm tra trước khi lên trình bày**

Thử tự trả lời các câu sau bằng lời của mình, rồi đối chiếu với phần tương ứng trong tài liệu:

1. Đầu vào của project là gì và đầu ra cần khác đầu vào như thế nào? Một quan sát khác một visit ở đâu? (Mục 0.)
2. Tại sao phải gom cùng tàu và sắp thời gian trước khi xác định visit? Repartition có tự sort không? (Mục 6.)
3. Tại sao 5 kịch bản tạo 6 visit, và candidate xuất hiện từ kịch bản nào? (Mục 4.)
4. Hai quan sát thật cách nhau bao lâu; vì sao reconciled nhưng vẫn censored hai đầu? (Mục 5.)
5. `PASS` kiểm tra những gì; độ chính xác thực tế, streaming và khả năng chịu lỗi đã được lệnh này kiểm tra chưa? (Mục 2 và 9.)
6. Dữ liệu tổng hợp, excerpt thật và pilot đầy đủ nằm ở đâu; con số nào thuộc tập nào? (Mục 7.)
7. Trong mã, hàm nào phân phối dữ liệu, hàm nào áp dụng quy tắc và hàm nào so đáp án? (Mục 11.)
8. Hôm nay đang demo phần nào của Lambda, phần nào còn ở kiến trúc/các mốc công bố sau? (Mục 8 và 12.)

Bạn đã nắm cốt lõi khi tự giải thích được: “Ta đang chuyển chuỗi quan sát vị trí thành các đợt hiện diện có điều kiện. Spark tổ chức dữ liệu theo tàu và thời gian; state machine tạo visit; fixture kiểm tra hành vi mong đợi; cờ bất định giúp không suy diễn quá mức từ dữ liệu thiếu.”
