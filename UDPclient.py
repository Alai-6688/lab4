import socket
import base64
import threading
import time
import os
from typing import Tuple, Optional

class UDPClient:
    def __init__(self, server_host: str, server_port: int, file_list_path: str):
        self.server_host = server_host
        self.server_port = server_port
        self.file_list_path = file_list_path
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket.settimeout(1.0)  # 初始超时时间1秒
    
    def send_and_receive(self, message: str, address: Tuple[str, int], max_retries: int = 5) -> Optional[str]:
        """发送消息并处理超时重传，返回响应或None（失败时）"""
        current_timeout = 1.0
        retries = 0
        
        while retries <= max_retries:
            try:
                self.client_socket.sendto(message.encode(), address)
                response, _ = self.client_socket.recvfrom(4096)
                return response.decode().strip()
            except socket.timeout:
                retries += 1
                current_timeout *= 2  # 指数退避
                self.client_socket.settimeout(current_timeout)
                print(f"超时，重试 {retries}/{max_retries}，超时时间: {current_timeout}秒")
            except Exception as e:
                print(f"发送/接收错误: {e}")
                return None
        
        print("达到最大重试次数，放弃")
        return None
    
    def download_file(self, filename: str, server_address: Tuple[str, int]) -> bool:
        """下载单个文件，返回是否成功"""
        # 发送DOWNLOAD请求
        download_msg = f"DOWNLOAD {filename}"
        response = self.send_and_receive(download_msg, server_address)
        
        if not response:
            return False
        
        # 处理响应
        parts = response.split()
        if parts[0] == "ERR" and parts[2] == "NOT_FOUND":
            print(f"错误：文件 {filename} 未找到")
            return False
        
        if parts[0] != "OK" or parts[1] != filename or "SIZE" not in parts or "PORT" not in parts:
            print(f"无效响应: {response}")
            return False
        
        # 解析文件大小和数据端口
        size_idx = parts.index("SIZE")
        port_idx = parts.index("PORT")
        file_size = int(parts[size_idx + 1])
        data_port = int(parts[port_idx + 1])
        data_address = (self.server_host, data_port)
        
        print(f"开始下载 {filename}，大小: {file_size} 字节")
        
        # 创建文件
        with open(filename, 'wb') as f:
            downloaded = 0
            block_size = 1000  # 每块最大1000字节
            
            # 分块下载
            while downloaded < file_size:
                start = downloaded
                end = min(downloaded + block_size - 1, file_size - 1)
                file_msg = f"FILE {filename} GET START {start} END {end}"
                
                response = self.send_and_receive(file_msg, data_address)
                if not response:
                    return False
                
                parts = response.split()
                if parts[0] != "FILE" or parts[1] != filename or parts[2] != "OK":
                    print(f"无效数据响应: {response}")
                    return False
                
                # 解析数据部分
                data_idx = parts.index("DATA")
                base64_data = " ".join(parts[data_idx+1:])
                try:
                    binary_data = base64.b64decode(base64_data)
                except Exception as e:
                    print(f"Base64解码错误: {e}")
                    return False
                
                # 写入文件
                f.write(binary_data)
                downloaded += len(binary_data)
                print(f"\r下载进度: {'*' * (downloaded // (file_size // 50))} {downloaded}/{file_size}字节", end="")
            
            print()  # 换行
            
            # 发送关闭请求
            close_msg = f"FILE {filename} CLOSE"
            response = self.send_and_receive(close_msg, data_address)
            if not response or not response.startswith(f"FILE {filename} CLOSE_OK"):
                print(f"关闭确认失败: {response or '无响应'}")
                return False
        
        print(f"文件 {filename} 下载完成")
        return True
    
    def run(self):
        """运行客户端，下载文件列表中的所有文件"""
        try:
            with open(self.file_list_path, 'r') as f:
                filenames = [line.strip() for line in f if line.strip()]
            
            if not filenames:
                print("文件列表为空")
                return
            
            server_address = (self.server_host, self.server_port)
            for filename in filenames:
                print(f"\n==== 开始下载 {filename} ====")
                success = self.download_file(filename, server_address)
                if not success:
                    print(f"下载 {filename} 失败")
        
        except FileNotFoundError:
            print(f"错误：文件列表 {self.file_list_path} 不存在")
        except Exception as e:
            print(f"运行错误: {e}")
        finally:
            self.client_socket.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("用法: python3 UDPclient.py <服务器主机> <服务器端口> <文件列表路径>")
        sys.exit(1)
    
    server_host, server_port, file_list_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    client = UDPClient(server_host, server_port, file_list_path)
    client.run()