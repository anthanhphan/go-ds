# Flow 03: Get File Metadata

Tài liệu này mô tả cách truy xuất thông tin mô tả file một cách nhất quán trong hệ thống storage không phân cấp.

## 1. Quy trình Step-by-Step

1.  **Xác định MetaID**: Hệ thống tạo một key đặc biệt đại diện cho metadata của file (thường là `meta_<fileID>`).
2.  **Định vị Node**: Dùng Consistent Hashing trên `metaID` để biết metadata này được lưu ở đâu.
3.  **Đọc có nhất quán**: Tương tự như đọc chunk, metadata cũng được replicate và đọc theo các mức độ `ReadConsistency`.
4.  **Giải mã**: Metadata được lưu ở dạng JSON (serialized struct `FileMetadata`), hệ thống thực hiện unmarshal để lấy object.

---

## 2. Minh họa Code & Giải thích

Phần xử lý lưu trữ metadata (`internal/api/service/metadata_service.go`):

```go
// SaveMetadata lưu trữ metadata lên các replica một cách nhất quán.
func (s *metadataService) SaveMetadata(ctx context.Context, metaID string, data []byte) error {
	consistency := s.core.getWriteConsistency()
	replicas, requiredSuccess, _ := s.resolveMetadataReplicas(metaID, consistency)

	// Tính toán checksum của metadata để đảm bảo khi đọc lại không bị sai lệch.
	checksum := crc32.ChecksumIEEE(data)
	
	// Thực thi việc ghi song song lên các replica.
	successCount, writeErr := s.core.executeReplicaWrites(
		ctx,
		replicas,
		requiredSuccess,
		func(execCtx context.Context, node shard.Node) error {
			// Metadata được coi như một chunk đặc biệt với tag "meta"
			resp, err := s.core.storage.WriteChunk(execCtx, node.Addr, metaID, "meta", checksum, bytes.NewReader(data))
			return err
		},
		// Callback xử lý lỗi trên node đơn lẻ
		func(node shard.Node, err error) error {
			return fmt.Errorf("meta write failed on node %s: %w", node.ID, err)
		},
	)
	return writeErr
}
```

**Tại sao xử lý như vậy?**
- **Symmetry**: Metadata được đối xử như một chunk dữ liệu thông thường. Điều này giúp tái sử dụng toàn bộ hạ tầng Storage, Replication và Anti-entropy hiện có mà không cần tạo thêm một layer database riêng cho Metadata.

---

## 3. Ưu & Nhược điểm

### Ưu điểm
- **Đơn giản hóa hệ thống**: Không cần cài thêm Postgres hay Redis để lưu metadata, giúp giảm sự phụ thuộc vào các hệ thống bên ngoài.
- **Consistent Scaling**: Metadata scales cùng với Data sharding.

### Nhược điểm
- **Search Limitation**: Vì metadata lưu theo key-value rải rác, việc thực hiện các query như "Liệt kê tất cả các file có đuôi .png" sẽ yêu cầu scan toàn bộ cluster (rất chậm).

---

## 4. Tối ưu tương lai
- **Global Index Layer**: Tích hợp một database chuyên dụng (như Cassandra hoặc ElasticSearch) chỉ để phục vụ việc search và lọc metadata nếu nhu cầu hệ thống yêu cầu tính năng duyệt file phức tạp.
