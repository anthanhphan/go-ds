# Flow 03 & 06: Metadata & Topology Detail

Tài liệu này chi tiết cách hệ thống cung cấp thông tin mô tả file và trạng thái cluster một cách hiệu quả.

## 1. Metadata Flow: End-to-End

### Quy trình Request
**Client** -> **HTTP Get /files/metadata** -> **API Service** -> **Metadata Service** -> **Consistent Hashing Lookup** -> **Multiple Storage Nodes** -> **Read LSM Storage**.

### Minh họa Code chi tiết
**File**: `internal/api/service/metadata_service.go`

```go
func (s *metadataService) getFileMetadata(ctx context.Context, fileID string) (*domain.FileMetadata, error) {
    // 1. Xác định ID của object metadata
    metaID := buildMetadataID(fileID)

    // 2. Tìm kiếm các node đang giữ replica của metadata này
    // Dữ liệu metadata cũng được sharded và replicated y hệt như data thường.
    replicas, requiredSuccess, _ := s.resolveMetadataReplicas(metaID, consistency)

    // 3. Thu thập metadata từ các replica song song (Parallel Collection)
    // Handoff: Gọi gRPC ReadChunk tới nhiều Storage Node cùng lúc
    for _, replica := range replicas {
        go func(node shard.Node) {
            meta, _ := s.readMetadataFromReplica(ctx, node, metaID)
            resultChan <- meta
        }(replica)
    }

    // Trả về bản ghi metadata đầu tiên hợp lệ thu thập được
    return <-resultChan, nil
}
```

---

## 2. Topology Flow: End-to-End

### Quy trình Request
**Admin/Uptime Tool** -> **HTTP Get /files/topology** (Hoặc qua gRPC) -> **Local Topology Service** -> **Local Shard Ring**.

### Minh họa Code chi tiết
**File**: `internal/storage/service/topology_service.go`

```go
func (s *topologyQueryService) getClusterTopology(ctx context.Context) ([]shard.Node, error) {
    // 1. Không cần đi ra mạng: Đọc trực tiếp từ Ring trong memory của node hiện tại.
    // Điều này cực kỳ quan trọng vì Topology API không được gây tải cho cluster.
    // Ring này được Gossip Adapter cập nhật liên tục ở background.
    return s.core.ring.GetNodes(), nil
}
```

**Tại sao xử lý như vậy?**
- **Observability with Zero Cost**: Hệ thống cung cấp cái nhìn toàn cảnh về cluster mà không tốn bất kỳ tài nguyên mạng nào (vì mỗi node đều có một bản copy local của Ring).
- **Consistency vs Availability**: Topology là có tính nhất quán cuối cùng (Eventual Consistency). Trong hầu hết các trường hợp monitoring, việc nhanh hơn vài mili-giây quan trọng hơn việc chính xác tuyệt đối từng node.

---

## 3. Tổng kết Công nghệ

- **Unified Storage Architecture**: Metadata không cần Database riêng (Postgres/Redis), được lưu trực tiếp vào LSM Tree của Storage Node dưới dạng chunk đặc biệt. Giúp hệ thống "Sạch" và dễ vận hành.
- **In-Memory Ring View**: Topology lookup là O(1) thao tác với RAM, đảm bảo các dashboard giám sát có thể refresh liên tục mà không làm ảnh hưởng performance hệ thống.
