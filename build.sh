#!/bin/bash

# 这是编译脚本, 自动更新glide依赖，编译程序并输出到GOPATH/output目录

if [ "$GOPATH" != `pwd` -a "$GOPATH" != `pwd`/ ];then
    echo "请设置GOPATH为项目根目录，并在GOPATH目录执行build.sh"
    exit 1
fi

if [ ! -f "bin/glide" ];then
    echo "请在项目根目录执行mkdir -p bin && curl https://glide.sh/get | sh"
    exit 1
fi

# 安装glide依赖
cd src/bigpipe && ../../bin/glide install && cd -

# 编译程序
cd src/bigpipe && go build && cd -

# 输出到output目录
rm -rf output
mkdir output

cp src/bigpipe/bigpipe output/
cp -r conf output/
mkdir -p output/logs