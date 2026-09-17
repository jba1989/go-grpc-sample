package main

import (
	"context"
	"fmt"
	"io"
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
	conn, err := grpc.NewClient(targetAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ 無法建立與 Service B 的連線通道: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("⚠️ 關閉連線時發生錯誤: %v", err)
		}
	}()

	// 步驟 2: 建立客戶端存根 (Stub)
	// 💡【核心觀念：什麼是 Stub？】
	// client 是由 Proto 自動生成的「服務存根 (Stub)」，就像是本地的「服務總機/代理人」。
	// 它的生命週期很長（通常與連線共存），封裝了 Proto 裡定義的所有 API 方法。
	// 不管是單向呼叫還是雙向串流，第一步都必須透過 Stub！
	client := pb.NewGreetingServiceClient(conn)

	// =========================================================================
	// 展示一：單向 RPC 呼叫 (Unary RPC)
	// =========================================================================
	log.Println("\n--- 【展示一：單向 RPC 呼叫 (Unary RPC)】---")
	ctxUnary, cancelUnary := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelUnary()

	req := &pb.HelloRequest{
		Name:    "Service A (用戶端)",
		Message: "你好 Service B，這是單向 Hello 測試！",
	}
	// 在單向 RPC 中，因為只有「一發一收」，gRPC 在底層自動幫你建立並關閉短暫的 stream，
	// 開發者直接透過 Stub 傳入 Request 並拿到 Response，感受不到 stream 的存在。
	resp, err := client.SayHello(ctxUnary, req)
	if err != nil {
		log.Fatalf("❌ Unary RPC 呼叫失敗: %v", err)
	}
	log.Printf("📥 [Service A] 收到單向回應: %s", resp.GetReply())

	// =========================================================================
	// 展示二：雙向串流 RPC (Bidirectional Streaming RPC)
	// =========================================================================
	log.Println("\n--- 【展示二：雙向串流 RPC (Bidirectional Streaming)】---")
	log.Println("💡 雙向串流特點：Client 與 Server 可以在同一個 HTTP/2 連線上，同時、非同步地相互收發訊息！")

	// 步驟 1: 透過 Stub 建立雙向串流通道 (Stream)
	// 💡【核心觀念：Stub 與 Stream 是不同的東西！】
	// - Stub 是「總機」；而 stream 是撥通後回傳的「專屬雙向通話語音線路」。
	// - 只要需要持續、多次收發，Stub 就會建立一個 stream 物件給我們。
	// - 後續所有收發 (stream.Send / stream.Recv) 都由這個專屬的 stream 負責！
	stream, err := client.Chat(context.Background())
	if err != nil {
		log.Fatalf("❌ 無法開啟雙向串流通道: %v", err)
	}

	// 建立一個 channel，用於當背景「接收 Goroutine」完成時通知主執行緒
	waitc := make(chan struct{})

	// 步驟 2: 啟動獨立的 Goroutine，在背景持續監聽並接收服務端推播的訊息
	// 這樣做能實現「收」與「發」完全解耦，達到真正的全雙工通訊 (Full-Duplex)
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// io.EOF 表示服務端也結束了串流傳輸
				log.Println("👋 [Service A Stream] 服務端已關閉串流 (收到 io.EOF)")
				close(waitc) // 通知主執行緒已全部接收完畢
				return
			}
			if err != nil {
				log.Fatalf("❌ [Service A Stream] 接收服務端訊息失敗: %v", err)
			}
			log.Printf("📥 [Service A Stream 收到回推] 來源: [%s] -> 訊息: 「%s」", in.GetSender(), in.GetMessage())
		}
	}()

	// 步驟 3: 主執行緒負責向服務端連續發送多筆訊息 (模擬即時事件或聊天訊息)
	messagesToSend := []string{
		"第一封：請求建立雙向即時資料串流管道",
		"第二封：即時遙測數據回報 -> CPU負載正常、記憶體使用量 32%",
		"第三封：心跳偵測訊號 (Heartbeat Ping)",
		"第四封：本日所有事件已同步完成，準備道別！",
	}

	for i, msgText := range messagesToSend {
		chatMsg := &pb.ChatMessage{
			Sender:    "Service A (用戶端)",
			Message:   fmt.Sprintf("[Seq #%d] %s", i+1, msgText),
			Timestamp: time.Now().Unix(),
		}

		log.Printf("📤 [Service A Stream 發送中] %s", chatMsg.GetMessage())
		if err := stream.Send(chatMsg); err != nil {
			log.Fatalf("❌ [Service A Stream] 發送串流訊息失敗: %v", err)
		}

		// 模擬訊息發送間隔
		time.Sleep(600 * time.Millisecond)
	}

	// 步驟 4: 客戶端訊息發送完畢，呼叫 CloseSend() 通知服務端「我不會再傳了」
	// ⚠️ 注意：CloseSend() 只關閉發送方向，接收方向依然保持暢通，直到服務端也結束
	log.Println("🚪 [Service A Stream] 本端發送完畢，呼叫 CloseSend() 半關閉連線...")
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("❌ 關閉發送串流失敗: %v", err)
	}

	// 步驟 5: 阻塞等待背景 Goroutine 接收完服務端最後的確認回應
	<-waitc
	log.Println("✅ [Service A Stream] 雙向串流通訊順利圓滿結束！")
}
