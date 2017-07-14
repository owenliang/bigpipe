package server

import (
	"net/http"
	"time"
	"bigpipe"
	"strconv"
	"bigpipe/log"
)

type Server struct {
	httpServer *http.Server	// HTTP服务器
	httpHandler *Handler	// 路由
}

// 创建服务器
func CreateServer(handler *Handler) *Server {
	srv := Server{httpHandler: handler}

	bigConf := bigpipe.GetConfig()

	// 创建HTTP服务器
	srv.httpServer = &http.Server{
		Addr:	":" + strconv.Itoa(bigConf.Http_server_port), // 监听端口
		ReadTimeout: time.Duration(bigConf.Http_server_read_timeout) * time.Millisecond,	// 读超时
		WriteTimeout: time.Duration(bigConf.Http_server_write_timeout) * time.Millisecond,	// 写超时
		Handler: handler.getMux(),	// 注册路由
	}
	return &srv;
}

func DestroyServer(server *Server) {
	if err := server.httpServer.Shutdown(nil); err != nil {
		log.ERROR("关闭HTTP服务器错误%v", err)
	} else {
		log.INFO("HTTP服务器关闭成功")
	}
}

// 运行服务器
func (srv *Server) Run() error {
	return srv.httpServer.ListenAndServe()
}



