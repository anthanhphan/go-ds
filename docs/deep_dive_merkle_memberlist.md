# Deep Dive: Merkle Tree & Memberlist (Gossip)

Tài liệu này phân tích chi tiết hai công cụ quan trọng nhất giúp hệ thống đạt được tính nhất quán và khả năng tự tổ chức mà không cần Master Node.

## 1. Merkle Tree Deep Dive (`pkg/merkle/tree.go`)

Merkle Tree trong dự án này được hiện thực dưới dạng **Fixed-size Heap-based Tree** với 1024 lá (buckets).

### A. Cấu trúc cây (Heap-based)
Cây được lưu bằng một mảng phẳng (`nodes []string`) để tối ưu hóa việc truy cập và quản lý bộ nhớ.
- Nút gốc (Root): Index `0`.
- Con trái: `2*i + 1`.
- Con phải: `2*i + 2`.

### B. Phân tích Code Chi tiết

```go
// UpdateBucket cập nhật hash của một bucket và lan tỏa lên Root.
func (t *MerkleTree) UpdateBucket(bucketID int, leafHash string) error {
    t.mu.Lock()
    defer t.mu.Unlock()

    // 1. Xác định vị trí của Bucket (Leaf) trong mảng phẳng
    idx := t.leafOffset + bucketID
    t.nodes[idx] = leafHash

    // 2. Vòng lặp "Bubble up": Tính lại hash từ lá lên tận gốc
    // Độ phức tạp chỉ là O(log N), với 1024 lá thì chỉ mất 10 lần tính hash.
    for idx > 0 {
        parentIdx := (idx - 1) / 2
        
        // Lấy hash của 2 con (trái và phải)
        left := t.nodes[parentIdx*2+1]
        right := t.nodes[parentIdx*2+2]

        // Hash cặp con để tạo hash cho cha
        t.nodes[parentIdx] = t.hashPair(left, right)
        
        // Nhảy lên tầng cha để tiếp tục
        idx = parentIdx
    }
    return nil
}

// hashPair thực hiện hash SHA256 kết hợp 2 node con.
func (t *MerkleTree) hashPair(left, right string) string {
    if left == "" && right == "" { return "" }
    h := sha256.New()
    h.Write([]byte(left))
    h.Write([]byte(right))
    return hex.EncodeToString(h.Sum(nil))
}
```

**Ứng dụng trong dự án:**
Dữ liệu mỗi node được chia thành 1024 buckets. Khi so sánh 2 node, chúng ta chỉ cần so sánh Root Hash. Nếu khác, ta so sánh các con để tìm ra chính xác bucket nào lỗi và chỉ cần sync lại bucket đó.

---

## 2. Memberlist & Gossip Deep Dive (`pkg/gossip/memberlist.go`)

Sử dụng giao thức Gossip để các node tự phát hiện lẫn nhau.

### A. Cơ chế Hoạt động
- **SWIM Protocol**: Mỗi node định kỳ chọn ngẫu nhiên một vài node khác để gửi heartbeat. 
- **Piggybacking**: Thông tin về node mới hoặc node vừa sập được "đính kèm" vào các gói tin heartbeat này, lan tỏa như một loại virus (gossip) trong cluster.

### B. Phân tích Code Chi tiết

```go
// NewGossipAdapter khởi tạo node và join vào cluster.
func NewGossipAdapter(...) {
    // 1. Cấu hình Memberlist (sử dụng DefaultLANConfig cho mạng nội bộ)
    config := memberlist.DefaultLANConfig()
    config.Name = nodeID
    config.BindAddr = bindAddr

    // 2. Đăng ký Delegate: Node này sẽ tự trả lời các câu hỏi về Metadata
    adapter := &GossipAdapter{...}
    config.Delegate = adapter 
    config.Events = adapter   

    // 3. Khởi tạo và Join
    list, _ := memberlist.Create(config)
    adapter.list = list

    // 4. Tự đăng ký mình vào Ring (Bản đồ Consistent Hashing local)
    ring.AddNode(localNode)
}

// NotifyJoin xử lý khi có node mới tham gia vào mạng lưới Gossip.
func (g *GossipAdapter) NotifyJoin(node *memberlist.Node) {
    // 1. Mỗi package Gossip kèm theo một metadata gọi là "Node Meta"
    // Giải mã để biết node này thuộc Shard nào, Port bao nhiêu.
    shardID, replicaID, serverPort := decodeMeta(node.Meta)
    
    // 2. Cập nhật Ring cục bộ
    // Khi Ring được cập nhật, logic sharding tự động thay đổi tại node hiện tại.
    g.ring.AddNode(shard.Node{
        ID: node.Name,
        Addr: net.JoinHostPort(addr, strconv.Itoa(serverPort)),
        ShardID: shardID,
    })
}
```

---

## 3. Sự kết hợp hoàn hảo

- **Gossip** giúp mọi node đồng thuận về **Topology** (Cluster có những ai, ở shard nào).
- **Consistent Hashing** biến Topology đó thành **Data Ownership** (File này thuộc về ai).
- **Merkle Tree** ngồi dưới cùng để canh gác **Data Integrity** (Node A và Node B có thực sự giữ dữ liệu giống nhau không).

Nếu một node mới Join via **Gossip**, nó sẽ thông báo cho các node khác là nó đã sẵn sàng. Các node cũ thấy `nodeID` này xuất hiện trong **Ring**, tính toán lại và thấy một số file nay thuộc về node mới. Quá trình **Anti-entropy (dùng Merkle)** sẽ phát hiện ra sự thiếu hụt này ở node mới và bắt đầu đẩy dữ liệu sang cho nó.

---

## 4. Tối ưu tương lai
- **V-Cells**: Phân vùng Gossip để giảm số lượng gói tin UDP khi cluster đạt quy mô > 500 nodes.
- **Merkle XOR**: Sử dụng phép toán XOR thay vì SHA256 cho các nút trung gian để tăng hiệu năng cập nhật cây Merkle.
