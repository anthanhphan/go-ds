# Deep Dive: LSM Tree Storage Engine

Tài liệu này phân tích chi tiết cách hiện thực hóa Log-Structured Merge (LSM) Tree trong dự án này, giải thích từng dòng code quan trọng và cách nó giải quyết bài toán lưu trữ phân tán hiệu suất cao.

## 1. Tại sao lại dùng LSM Tree?
Trong hệ thống phân tán, hiệu suất ghi (`Write Throughput`) thường là bottleneck. LSM Tree chuyển đổi các thao tác ghi ngẫu nhiên (Random Write - vốn rất chậm trên HDD và hại SSD) thành ghi tuần tự (Sequential Write - cực nhanh).

---

## 2. Phân tích chi tiết LSMAdapter (`internal/storage/adapter/outbound/lsm/lsm.go`)

### A. Cấu trúc dữ liệu và Khởi tạo
LSMAdapter kết hợp giữa việc ghi file tuần tự và một Index trong memory để tra cứu nhanh.

```go
type LSMAdapter struct {
    indexMu         sync.RWMutex
    fileMu          sync.Mutex
    dirPath         string
    activeFile      *os.File       // File segment đang mở để ghi
    activeFileID    uint64         // ID của segment hiện tại
    index           map[domain.ChunkID]IndexEntry // Map ánh xạ ID -> Vị trí trên đĩa
    merkleTree      *merkle.MerkleTree // Cây Merkle gắn liền để phục vụ sync
}
```

**Khởi tạo (`NewLSMAdapter`):**
Hệ thống luôn chạy lệnh `replayLogs()` khi khởi động để đảm bảo tính nhất quán của Index trong RAM.

---

### B. Cơ chế Ghi dữ liệu (`WriteChunk`) - Comment chi tiết từng dòng

```go
func (a *LSMAdapter) WriteChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error {
    // 1. Check trùng lặp (Idempotency) - Nếu đã có thì không ghi nữa
    a.indexMu.RLock()
    _, exists := a.index[id]
    a.indexMu.RUnlock()
    if exists { return nil }

    // 2. Lock file để đảm bảo chỉ 1 thread ghi vào cuối file tại một thời điểm
    a.fileMu.Lock()
    defer a.fileMu.Unlock()

    // 3. Nhảy tới cuối file (Append-only)
    offset, _ := a.activeFile.Seek(0, io.SeekEnd)

    // 4. Ghi dữ liệu theo format tuần tự: [ID_Len | ID | Data_Len | Data | Checksum]
    // Việc ghi tuần tự này cực kỳ nhanh vì đầu từ của đĩa không phải nhảy đi chỗ khác
    header := make([]byte, 4+len(idBytes)+4)
    binary.BigEndian.PutUint32(header[0:4], idLen)
    copy(header[4:4+len(idBytes)], idBytes)
    a.activeFile.Write(header) // Ghi header
    written, _ := io.Copy(a.activeFile, reader) // Stream data trực tiếp từ mạng vào đĩa

    // 5. Cập nhật Index trong memory (O(1))
    a.indexMu.Lock()
    a.index[id] = IndexEntry{
        SegmentID: a.activeFileID,
        Offset:    offset,
        Size:      totalEntrySize,
        Checksum:  checksum,
    }
    a.indexMu.Unlock()

    // 6. Cập nhật Merkle Tree để phục vụ việc so sánh dữ liệu giữa các node
    a.updateMerkleTree(id)

    // 7. Rotation: Nếu file segment quá lớn (64MB), đóng lại và mở file mới
    if info.Size() > a.maxSegmentSize {
        a.rotateActiveFile()
    }
    return nil
}
```

---

### C. Cơ chế Đọc dữ liệu (`ReadChunk`)
Đọc dữ liệu trong LSM cực nhanh nhờ Index luôn nằm trên RAM.

```go
func (a *LSMAdapter) ReadChunk(ctx context.Context, id domain.ChunkID) (uint32, io.ReadCloser, error) {
    // 1. Tìm vị trí (SegmentID, Offset) từ Index trong RAM - Mất O(1)
    a.indexMu.RLock()
    entry, exists := a.index[id]
    a.indexMu.RUnlock()

    // 2. Mở file segment tương ứng
    f, _ := os.Open(a.getSegmentPath(entry.SegmentID))

    // 3. Dùng ReadAt để đọc dữ liệu tại đúng vị trí mà không ảnh hưởng thread khác
    // 4. Trả về một LimitedReader để streaming dữ liệu ra ngoài
    return entry.Checksum, io.LimitReader(f, entry.Size), nil
}
```

---

### D. Cơ chế Gộp file (`Compaction`)
Vì là Append-only, các bản dữ liệu cũ hoặc chunk đã xóa vẫn nằm trên đĩa. `Compaction` giúp thu hồi không gian.

1.  **Ghi đè thông minh**: Hệ thống tạo một thư mục tạm, đọc tất cả các chunk "sống" (có trong Index hiện tại) và ghi chúng vào các segment mới tối ưu hơn.
2.  **Cập nhật Index Atomatic**: Sau khi ghi xong các segment mới, hệ thống tráo đổi (swap) Index cũ bằng Index mới và xóa các file segment cũ.
3.  **Correctness**: Trong lúc Compaction chạy, các đợt Write mới vẫn tiếp tục vào `activeFile` mới, không bị gián đoạn.

---

## 3. Ứng dụng trong Dự án

Trong dự án này, LSM Tree đóng vai trò là "Trái tim" của storage node:
- **Xử lý Chunking**: Mỗi mảnh của file là một bản ghi trong LSM.
- **Tích hợp Merkle**: Mỗi lần LSM thay đổi, nó tự động "báo" cho Merkle Tree để tính lại Root hash.
- **Replay trên startup**: Giúp hệ thống khôi phục trạng thái cực nhanh sau khi crash mà không mất dữ liệu (do dữ liệu đã nằm an toàn trong Write-ahead Log / Segments).

---

## 4. Tối ưu tương lai
- **Bloom Filters**: Thêm Bloom Filter vào đầu mỗi segment để biết nhanh một Key có tồn tại trong segment đó không mà không cần đọc Index (nếu Index quá lớn phải swap ra đĩa).
- **Multi-level Segments**: Áp dụng Leveling (như LevelDB) để tối ưu hơn nữa quá trình Compaction.
