# System Architecture: Distributed File Storage

Hệ thống lưu trữ file phân tán được thiết kế theo mô hình **Masterless** (không có node điều phối trung tâm), sử dụng **Consistent Hashing** để phân bổ dữ liệu và **Gossip Protocol** để quản lý trạng thái cluster.

## 1. Flow: Upload File
Dùng để đẩy một file từ client vào hệ thống, đảm bảo tính sẵn sàng cao thông qua replication.

**Step-by-step:**
1. **ID Generation**: Hệ thống sử dụng **Snowflake ID** để tạo `fileID` duy nhất trên toàn cluster.
2. **Chunking**: File được đọc dưới dạng stream và chia thành các chunk cố định (mặc định 64MB - cấu hình tại `cfg.App.ChunkSize`).
3. **Parallel Replication**: 
   - Với mỗi chunk, hệ thống xác định danh sách các node replicas dựa trên consistent hashing.
   - Các chunk được upload song song lên các node này (`upload_service.go:127`).
4. **Consistency Enforcement**: Chunk được coi là upload thành công nếu số lượng node ACK >= `WriteConsistency` (One, Quorum, All).
5. **Metadata Persistence**: Sau khi tất cả các chunk thành công, metadata của file (tên, size, extension) được lưu trữ riêng biệt dưới dạng một meta-chunk trên cluster.
6. **Cleanup**: Nếu có lỗi xảy ra, hệ thống tự động gọi routine xóa các chunk "mồ côi" để giải phóng tài nguyên.

**Công nghệ đặc biệt**: Parallel Worker Pool (resilience package), Snowflake ID, Murmur3 Hashing.

---

## 2. Flow: Download File
Phục hồi lại file từ các chunk đã lưu trữ rải rác.

**Step-by-step:**
1. **Get Metadata**: Truy vấn metadata của `fileID` để biết số lượng chunk và đặc tính file.
2. **Chunk Retrieval with Consistency**:
   - Đối với từng index của chunk, hệ thống tìm các node đang giữ replica.
   - Thực hiện đọc chunk từ các replica theo mức độ `ReadConsistency`.
3. **Checksum Verification**: Mỗi chunk khi đọc về được kiểm tra CRC32 so với checksum lưu trữ tại node storage để đảm bảo tính toàn vẹn dữ liệu.
4. **Streaming Reconstruct**: Dữ liệu từ các chunk được nối lại và ghi trực tiếp vào `io.Writer` của client.

---

## 3. Flow: Get File Metadata
Lấy thông tin mô tả về file mà không cần đọc dữ liệu thực tế.

**Step-by-step:**
1. Xác định `metaID` dựa trên `fileID`.
2. Truy vấn các storage node giữ metadata này bằng Consistent Hashing.
3. Giải mã dữ liệu JSON từ storage để trả về struct `FileMetadata`.

---

## 4. Flow: Sync Data (Anti-entropy)
Đảm bảo tính nhất quán cuối cùng (Eventual Consistency) giữa các replica.

**Cơ chế chính**: Merkle Tree Reconciliation.
1. **Intra-shard (Trong cùng Shard)**: 
   - Node định kỳ so sánh `Merkle Root` với các peer trong cùng shard.
   - Nếu Root khác nhau, hệ thống thực hiện "khoan sâu" (drill-down) vào các node con của Merkle Tree để tìm bucket bị sai lệch.
   - Khi tìm thấy bucket sai, node sẽ yêu cầu list các item trong bucket đó và kéo (pull) những chunk bị thiếu hoặc sai checksum về.
2. **Cross-shard (Handoff)**: 
   - Kiểm tra các item mà node peer đang giữ nhưng thực tế thuộc về quyền sở hữu của node hiện tại (do cluster thay đổi topology).
   - Tiến hành kéo dữ liệu về node mình (Hinted Handoff).

---

## 5. Quản lý Memberlist
Hệ thống sử dụng **Gossip Protocol** (thư viện `hashicorp/memberlist`) để duy trì danh sách các node đang hoạt động.

**Đặc điểm**:
- **Discovery**: Các node mới sử dụng danh sách `seed_nodes` để join vào cluster.
- **Health Check**: Các node gửi gói tin "nói chuyện" với nhau định kỳ để phát hiện node nào "chết" hoặc "sống".
- **Metadata Exchange**: Thông tin về `ShardID`, `ReplicaID` và `ServerPort` được truyền qua payload của Gossip.
- **Ring Integration**: Khi Gossip phát hiện thay đổi (Join/Leave), nó tự động cập nhật vào `shard.Ring` (Consistent Hashing ring) để điều chỉnh việc phân bổ request.

---

## 6. Get Topology
Cung cấp cái nhìn tổng quan về trạng thái cluster.

**Cơ chế**:
- Truy vấn trực tiếp từ `shard.Ring` cục bộ của node.
- Trả về danh sách tất cả các node: ID, Address, ShardID, Status (Healthy/Unhealthy).

---

## LSM Tree & Merkle Tree: Hoạt động như thế nào?

### LSM Tree (Log-Structured Merge-Tree)
Storage engine được tối ưu cho **Write-Heavy workloads**.
- **Append-only Logs**: Dữ liệu được ghi tuần tự vào các file `segment_XXXXX.log`. Việc ghi tuần tự giúp đạt throughput cực cao cho ổ đĩa.
- **In-memory Index**: Lưu vị trí (offset, size) của các chunk trong memory để tìm kiếm tức thì.
- **Compaction**: Khi số lượng segment vượt ngưỡng, một background process sẽ chạy để gộp các segment, loại bỏ dữ liệu đã bị xóa hoặc ghi đè, giúp thu hồi dung lượng đĩa.
- **Persistence**: Index được snapshot định kỳ vào file `index.gob` để khởi động nhanh.

### Merkle Tree
Được sử dụng để **nhận diện sự sai lệch dữ liệu một cách hiệu quả**.
- **Heap-based storage**: Tree được lưu dưới dạng array phẳng (flattened array) để tối ưu memory.
- **Bucket Mapping**: Mỗi `chunkID` được hash (Murmur3) vào một trong 1024 bucket (leaf nodes).
- **Update propagation**: Khi một chunk được ghi vào LSM, bucket tương ứng sẽ tính lại hash (sha256 của tất cả chunk trong đó), sau đó propagate hash này lên các node cha cho tới Root.
- **Efficiency**: Chỉ cần so sánh hash Root là biết 2 node có giống nhau hoàn toàn hay không. Nếu khác, chỉ cần so sánh log(N) node để tìm ra chính xác dữ liệu cần sync.

---

## Đánh giá & Tối ưu tương lai

### Ưu điểm
- **Scalability**: Thiết kế masterless giúp mở rộng cluster không giới hạn mà không bị bottleneck tại node master.
- **Fault Tolerance**: Replication và Anti-entropy đảm bảo dữ liệu không bị mất ngay cả khi nhiều node gặp sự cố.
- **Write Performance**: LSM Tree tận dụng tối đa tốc độ ghi tuần tự.

### Nhược điểm & Tối ưu
1. **Merkle Scan**: Hiện tại việc cập nhật Merkle Tree yêu cầu scan toàn bộ item trong bucket (O(N) trong bucket).
   - *Tối ưu*: Duy trì hash cộng dồn để updates Merkle Tree trong O(1).
2. **Index Memory**: Toàn bộ key chunk nằm trên RAM.
   - *Tối ưu*: Sử dụng Bloom Filter và disk-based index (như SSTables trong LevelDB/RocksDB) khi số lượng file cực lớn.
3. **Consistency Level**: Hiện tại client phải tự hiểu về ý nghĩa của One/Quorum/All.
   - *Tối ưu*: Cung cấp các chính sách tự động retry hoặc hedging requests để giảm latency tail.
