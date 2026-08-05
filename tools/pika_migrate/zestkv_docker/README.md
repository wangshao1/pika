# 构建zestkv pika镜像

## 1. 删除output buildtrees deps 文件夹

``` bash
rm -rf output buildtrees deps
```

## 2. sed 命令替换依赖下载链接为内部链接

``` bash
sed -i "s/https:\/\/github.com/http:\/\/10.224.129.40:8000\/https:\/\/github.com/g" CMakeLists.txt
```

cd 到zestkv_docker目录下

``` bash
cd zestkv_docker
```

## 3. 构建基础镜像

1. 构建 pika 编译镜像

``` bash
sudo docker build -t zestkv/pika_builder:ubuntu_22.04 -f ./Dockerfile_builder .
```

2. 构建 pika 执行镜像

``` bash
sudo docker build -t zestkv/pika_runner:ubuntu_22.04 -f ./Dockerfile_runner .
```

## 4. 构建pika镜像

``` bash
sudo docker build -t zestkv/pika_ubuntu:0.3 -f Dockerfile_pika ..
```

## 5. 重新打tag，上传到公司 docker images 仓库

``` bash
sudo docker tag zestkv/pika_ubuntu:0.3 harbor.qihoo.net/jcjgz-zestkv/zestkv/pika_ubuntu:0.3
```