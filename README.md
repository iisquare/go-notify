# go-notify
业务数据变动通知

## GoLand

- Settings -> Go -> Go Modules(vgo) -> Enable Go Modules(vgo) integration

## Run

- download dependence
```
cd /path/to/project
go mod download
```
- build & install
```
# go install [-o 输出名] /path/to/main.go
go install ./...
```
- run with program arguments
```
cd $GOPATH
./bin/notify -c=/path/to/config.yml
```

## Tips

- 开发和部署环境均使用全局GOPATH以避免重复下载依赖，编译安装文件可手动指定目标位置
- [canal-go](https://github.com/CanalClient/canal-go/)部分文件命名异常，可手动解压到对应目录
- golang.org需要通过代理访问，对存在问题的依赖也可以通过镜像方式解决
```
GOPROXY=https://goproxy.io
```
- canal的go客户端暂不支持通过zookeeper获取server节点，可以参考java版自己实现
