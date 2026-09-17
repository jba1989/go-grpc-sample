package main

import (
	"context"
	"log"
	"time"

	// 匯入由 protoc 生成的 gRPC 與 Protobuf 結構體套件
	"grpc-sample/proto/pb"

	// 匯入官方 gRPC 核心套件與傳輸憑證設定
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 定義目標服務端 (Service B) 的位址與連接埠
const targetAddress = "localhost:50051"

func main() {
	log.Println("🚀 [Service A] 正在初始化 gRPC 用戶端...")

	// 步驟 1: 與服務端 (Service B) 建立連線通道 (ClientConn)
	// 在最新版 gRPC-Go 中，推薦使用 grpc.NewClient 代替過往的 grpc.Dial
	// insecure.NewCredentials() 表示使用純文字未加密連線 (適用於本地開發與內部測試環境)
	conn, err := grpc.NewClient(targetAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ 無法建立與 Service B 的連線通道: %v", err)
	}
	// 確保程式結束或退出前釋放連線資源
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("⚠️ 關閉連線時發生錯誤: %v", err)
		}
	}()

	// 步驟 2: 透過連線通道建立 GreetingService 的客戶端存根 (Stub)
	// Stub 是 gRPC 在客戶端自動生成的代理物件，封裝了網路請求細節，
	// 讓我們能像呼叫本地函式一樣呼叫遠端方法
	client := pb.NewGreetingServiceClient(conn)

	// 步驟 3: 建立具備「逾時控制」的 Context
	// 分散式系統中，網路可能延遲或服務端可能卡死，強烈建議每次 RPC 呼叫都設定 Timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 步驟 4: 組織請求參數 (Request Payload)
	req := &pb.HelloRequest{
		Name:    "Service A (用戶端)",
		Message: "你好 Service B，我是 Service A，我們已透過 gRPC 成功連線！",
	}

	log.Printf("📤 [Service A] 準備發送 RPC 請求 -> 目標: %s", targetAddress)
	log.Printf("   發送內容: Name=[%s], Message=[%s]", req.GetName(), req.GetMessage())

	// 步驟 5: 發起 SayHello 遠端過程呼叫 (RPC)
	// 此呼叫會被 gRPC 序列化為二進位 Protobuf，經由 HTTP/2 傳輸到 Service B
	resp, err := client.SayHello(ctx, req)
	if err != nil {
		log.Fatalf("❌ RPC 呼叫失敗: %v", err)
	}

	// 步驟 6: 處理並展示 Service B 回傳的結果
	replyTime := time.Unix(resp.GetTimestamp(), 0).Format("2006-01-02 15:04:05")
	log.Println("📥 [Service A] 成功收到 Service B 的回應！")
	log.Printf("   回應訊息 (Reply): %s", resp.GetReply())
	log.Printf("   處理時間 (Timestamp): %s (Unix: %d)", replyTime, resp.GetTimestamp())
}
