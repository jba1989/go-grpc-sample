package main

import (
	"context"
	"fmt"
	"io"
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

// -------------------------------------------------------------
// 1. 單向 RPC (Unary RPC) 實作
// -------------------------------------------------------------

// SayHello 是單向 RPC 方法：接收單一請求，回傳單一回應
func (s *server) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	clientName := req.GetName()
	clientMsg := req.GetMessage()

	log.Printf("📥 [Service B Unary] 收到請求 -> 來源: [%s], 訊息: [%s]", clientName, clientMsg)

	replyText := fmt.Sprintf("你好 %s！我是 Service B，我已經收到你的訊息：「%s」", clientName, clientMsg)
	currentTime := time.Now().Unix()

	resp := &pb.HelloResponse{
		Reply:     replyText,
		Timestamp: currentTime,
	}

	log.Printf("📤 [Service B Unary] 回傳回應 -> [%s]", replyText)
	return resp, nil
}

// -------------------------------------------------------------
// 2. 雙向串流 RPC (Bidirectional Streaming RPC) 實作
// -------------------------------------------------------------

// Chat 實作雙向串流邏輯
//
// 參數說明：
//   - stream: 雙向串流管道 (pb.GreetingService_ChatServer)，
//     同時具備 stream.Recv() 與 stream.Send() 能力。
//
// 運作機制：
//   1. 服務端透過一個無限迴圈持續呼叫 stream.Recv() 讀取用戶端發過來的訊息。
//   2. 當用戶端呼叫 stream.CloseSend() 關閉發送時，Recv() 會收到 io.EOF 錯誤，代表用戶端訊息已傳送完畢。
//   3. 在此期間，服務端可隨時、多次透過 stream.Send() 向用戶端發送回覆，彼此收發完全獨立且非同步！
func (s *server) Chat(stream pb.GreetingService_ChatServer) error {
	log.Println("🔄 [Service B Stream] 雙向串流通道已建立，準備接收訊息...")

	for {
		// 1. 持續監聽並接收用戶端發來的訊息
		in, err := stream.Recv()
		if err == io.EOF {
			// io.EOF 表示用戶端已關閉發送串流 (用戶端發送完成)
			log.Println("👋 [Service B Stream] 用戶端已結束傳輸 (收到 io.EOF)，關閉本次串流連線。")
			return nil
		}
		if err != nil {
			log.Printf("❌ [Service B Stream] 讀取串流訊息失敗: %v", err)
			return err
		}

		// 2. 處理收到的訊息
		log.Printf("📥 [Service B Stream] 收到來自 [%s] 的串流訊息: 「%s」", in.GetSender(), in.GetMessage())

		// 3. 組織伺服端的回應訊息並即時回推給用戶端
		replyMsg := fmt.Sprintf("Service B 已收到你的第 %s 號訊號！", in.GetMessage())
		resp := &pb.ChatMessage{
			Sender:    "Service B (服務端)",
			Message:   replyMsg,
			Timestamp: time.Now().Unix(),
		}

		// 透過同一個 stream 即時回傳
		if err := stream.Send(resp); err != nil {
			log.Printf("❌ [Service B Stream] 回傳串流訊息失敗: %v", err)
			return err
		}
		log.Printf("📤 [Service B Stream] 已即時回推回應給用戶端: [%s]", replyMsg)
	}
}

func main() {
	log.Println("🚀 [Service B] 正在啟動 gRPC 服務端...")

	// 步驟 1: 建立 TCP 網路監聽器 (Listener)，監聽指定的 port
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("❌ 無法監聽連接埠 %s: %v", port, err)
	}

	// 步驟 2: 建立 gRPC 伺服器實例
	grpcServer := grpc.NewServer()

	// 步驟 3: 將我們實作的業務邏輯 (&server{}) 註冊到 gRPC 伺服器中
	// 這一步會同時註冊 SayHello (單向) 與 Chat (雙向串流) 兩個方法
	pb.RegisterGreetingServiceServer(grpcServer, &server{})

	log.Printf("✅ [Service B] gRPC 伺服器已成功啟動，正在監聽 %s", port)

	// 步驟 4: 開始接受連線並提供服務 (阻塞運行，直到伺服器停止或發生錯誤)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ gRPC 伺服器運行失敗: %v", err)
	}
}
