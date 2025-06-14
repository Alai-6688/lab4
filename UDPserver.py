import socket
import base64
import os
import threading
import random
import time

class UDPServer:
    def __init__(self, server_port):
        self.server_port = server_port
        self.welcome_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.welcome_socket.bind(('0.0.0.0', server_port))
        self.data_ports = set()  # 记录已使用的数据端口
        print(f"服务器启动，监听端口: {server_port}")

    def get_random_data_port(self):
        """获取50000-51000之间的随机端口"""
        while True:
            port = random.randint(50000, 51000)
            if port not in self.data_ports:
                self.data_ports.add(port)
                return port

    def handle_client_request(self, request, client_addr):
        """处理客户端的DOWNLOAD请求"""
        request_str = request.decode().strip()
        parts = request_str.split()
        
        if len(parts) >= 2 and parts[0] == "DOWNLOAD":
            filename = parts[1]
            file_path = os.path.join(os.getcwd(), filename)
            
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                data_port = self.get_random_data_port()
                response = f"OK {filename} SIZE {file_size} PORT {data_port}"
                self.welcome_socket.sendto(response.encode(), client_addr)
                print(f"发送OK响应，文件: {filename}，大小: {file_size}，数据端口: {data_port}")
                
                # 创建新线程处理数据传输
                threading.Thread(
                    target=self.handle_file_transmission,
                    args=(filename, client_addr, data_port)
                ).start()
            else:
                response = f"ERR {filename} NOT_FOUND"
                self.welcome_socket.sendto(response.encode(), client_addr)
                print(f"发送错误响应，文件不存在: {filename}")
        else:
            print(f"无效请求: {request_str}")

    def handle_file_transmission(self, filename, client_addr, data_port):
        """处理单个客户端的文件数据传输"""
        file_path = os.path.join(os.getcwd(), filename)
        try:
            # 创建数据传输socket
            data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            data_socket.bind(('0.0.0.0', data_port))
            data_socket.settimeout(5.0)  # 设置超时，防止线程阻塞
            
            print(f"新线程启动，处理文件: {filename}，数据端口: {data_port}")
            
            # 打开文件读取
            with open(file_path, 'rb') as f:
                file_size = os.path.getsize(file_path)
                print(f"文件大小: {file_size}字节")
                
                while True:
                    try:
                        request, _ = data_socket.recvfrom(4096)
                        request_str = request.decode().strip()
                        parts = request_str.split()
                        
                        if len(parts) >= 6 and parts[0] == "FILE" and parts[1] == filename:
                            if parts[2] == "GET":
                                # 处理数据请求
                                start = int(parts[parts.index("START") + 1])
                                end = int(parts[parts.index("END") + 1])
                                print(f"收到数据请求，范围: {start}-{end}")
                                
                                if start < 0 or end >= file_size or start > end:
                                    # 无效范围，忽略
                                    continue
                                
                                # 读取指定范围的数据
                                f.seek(start)
                                data = f.read(end - start + 1)
                                base64_data = base64.b64encode(data).decode()
                                
                                response = f"FILE {filename} OK START {start} END {end} DATA {base64_data}"
                                data_socket.sendto(response.encode(), client_addr)
                                print(f"发送数据块，范围: {start}-{end}，大小: {len(data)}字节")
                            
                            elif parts[2] == "CLOSE":
                                # 处理关闭请求
                                response = f"FILE {filename} CLOSE_OK"
                                data_socket.sendto(response.encode(), client_addr)
                                print(f"发送关闭确认，文件: {filename}")
                                break
                        else:
                            print(f"无效的文件请求: {request_str}")
                    except socket.timeout:
                        # 超时，继续等待请求
                        continue
        except Exception as e:
            print(f"处理文件传输时出错: {e}")
        finally:
            # 释放端口
            if data_port in self.data_ports:
                self.data_ports.remove(data_port)
            data_socket.close()
            print(f"线程结束，文件: {filename}，数据端口: {data_port}")

    def run(self):
        """运行服务器，监听客户端请求"""
        try:
            while True:
                request, client_addr = self.welcome_socket.recvfrom(4096)
                print(f"收到客户端请求，地址: {client_addr}")
                # 使用线程处理请求，确保多客户端并发处理
                threading.Thread(
                    target=self.handle_client_request,
                    args=(request, client_addr)
                ).start()
        except Exception as e:
            print(f"服务器运行时出错: {e}")
        finally:
            self.welcome_socket.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("用法: python3 UDPserver.py <服务器端口>")
        sys.exit(1)
    
    server_port = int(sys.argv[1])
    server = UDPServer(server_port)
    server.run()