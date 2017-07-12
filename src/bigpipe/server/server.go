package server

import (
	"net/http"
	"time"
)

type Server struct {
	httpServer *http.Server	// HTTP服务器
	httpHandler *Handler	// 路由
}

// 创建服务器
func CreateServer(handler *Handler) *Server {
	srv := Server{httpHandler: handler}

	// 创建HTTP服务器
	srv.httpServer = &http.Server{
		Addr:	":10086",				// 监听端口
		ReadTimeout: 5 * time.Second,	// 读超时
		WriteTimeout: 5 * time.Second,	// 写超时
		Handler: handler.getMux(),	// 注册路由
	}
	return &srv;
}

// 运行服务器
func (srv *Server) Run() error {
	return srv.httpServer.ListenAndServe()
}



