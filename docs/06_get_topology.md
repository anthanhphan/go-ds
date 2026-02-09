# Flow 06: Get Topology

Cung cấp khả năng quan sát (Observability) cấu trúc cluster hiện tại.

## 1. Quy trình Step-by-Step

1.  **Query local Ring**: Request không cần đi ra ngoài mạng, nó truy vấn bản sao `shard.Ring` đang nằm trong bộ nhớ của chính node đó.
2.  **Node Mapping**: Lấy danh sách toàn bộ các node đã đăng ký qua Gossip.
3.  **Status Report**: Trả về trạng thái chi tiết của từng node (Healthy/Unhealthy).

---

## 2. Minh họa Code & Giải thích

Phần xử lý query topology (`internal/storage/service/topology_service.go`):

```go
// getClusterTopology trả về view hiện tại về các storage node từ ring cục bộ.
func (s *topologyQueryService) getClusterTopology(ctx context.Context) ([]shard.Node, error) {
    // Không có logic phức tạp ở đây vì Ring đã được cập nhật liên tục bởi Gossip background threads.
    // Việc đọc từ local memory giúp API này phản hồi cực nhanh (micro-seconds).
	return s.core.ring.GetNodes(), nil
}
```

**Tại sao xử lý như vậy?**
- **Performance**: API này thường được dùng bởi các tool monitoring hoặc dashboard. Việc đọc từ memory đảm bảo monitoring không gây tải thêm cho IO của hệ thống.

---

## 3. Ưu & Nhược điểm

### Ưu điểm
- **Real-time View**: Phản ánh chính xác những gì node "thấy" tại thời điểm đó.
- **Zero Network Cost**: Không cần giao tiếp xuyên node để trả về kết quả.

### Nhược điểm
- **Local View Inconsistency**: Trong lúc cluster đang biến động lớn (nhiều node chết cùng lúc), kết quả trả về từ Node A có thể hơi khác Node B một chút cho đến khi Gossip hội tụ (converge).

---

## 4. Tối ưu tương lai
- **Historical Topology**: Lưu trữ lịch sử thay đổi topology để phục vụ việc debug lỗi phân bổ dữ liệu trong quá khứ.
- **Replica Health Visualization**: Tích hợp thêm thông tin về tình trạng sử dụng disk và CPU của từng node vào topology response.
