package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	// 匯入由 protoc 生成的 gRPC 與 Protobuf 結構體套件
	"grpc-sample/proto/pb"

	// 匯入官方 gRPC 核心套件
	"google.golang.org/grpc"
)

// 定義服務端監聽的通訊埠
const port = ":50051"

// server 結構體用於實作 GreetingServiceServer 介面
// 嵌入 pb.UnimplementedGreetingServiceServer 是官方推薦的最佳實踐，
// 能確保未來 proto 介面新增方法時，即使尚未實作也不會導致編譯失敗（提供向前相容性）
type server struct {
	pb.UnimplementedGreetingServiceServer
}

// SayHello 是我們在 proto 檔案中定義的 RPC 方法在 Go 端的具體實作
//
// 參數說明：
//   - ctx: 呼叫上下文 (Context)，可用於處理逾時控制、取消訊號或傳遞 Metadata
//   - req: 由 Service A (Client) 傳遞過來的請求物件指標 (*pb.HelloRequest)
//
// 回傳說明：
//   - *pb.HelloResponse: 欲回傳給 Service A 的回應物件指標
//   - error: 若處理過程發生錯誤則回傳 error，成功時回傳 nil
func (s *server) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	// 1. 從請求物件中讀取資料 (建議使用 Getter 方法如 GetName()，具備 nil 防禦保護)
	clientName := req.GetName()
	clientMsg := req.GetMessage()

	log.Printf("📥 [Service B] 收到請求 -> 來源名稱: [%s], 附加訊息: [%s]", clientName, clientMsg)

	// 2. 組織回應文字與當前處理的時間戳記
	replyText := fmt.Sprintf("你好 %s！我是 Service B，我已經收到你的訊息：「%s」", clientName, clientMsg)
	currentTime := time.Now().Unix()

	// 3. 建立並回傳 Proto 產生的 Response 結構體
	resp := &pb.HelloResponse{
		Reply:     replyText,
		Timestamp: currentTime,
	}

	log.Printf("📤 [Service B] 回傳回應 -> 回應內容: [%s]", replyText)
	return resp, nil
}

func main() {
	log.Println("🚀 [Service B] 正在啟動 gRPC 服務端...")

	// 步驟 1: 建立 TCP 網路監聽器 (Listener)，監聽指定的 port
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("❌ 無法監聽連接埠 %s: %v", port, err)
	}

	// 步驟 2: 建立 gRPC 伺服器實例
	// 這裡可以透過 grpc.NewServer(opts...) 傳入攔截器 (Interceptor)、TLS 設定等選項
	grpcServer := grpc.NewServer()

	// 步驟 3: 將我們實作的業務邏輯 (&server{}) 註冊到 gRPC 伺服器中
	// 這一步會將 Proto 定義的方法路由綁定至該實體
	pb.RegisterGreetingServiceServer(grpcServer, &server{})

	log.Printf("✅ [Service B] gRPC 伺服器已成功啟動，正在監聽 %s", port)

	// 步驟 4: 開始接受連線並提供服務 (阻塞運行，直到伺服器停止或發生錯誤)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ gRPC 伺服器運行失敗: %v", err)
	}
}
