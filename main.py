# 文件名: main.py (此文件上传到 GitHub Gist)

import os
import requests
import subprocess
import threading
import time
import uuid
import socket
from flask import Flask, request, jsonify, Response
from urllib.parse import urljoin, quote, unquote
import json
import base64
import hashlib

# 尝试导入必要的库，并优雅地处理错误
try:
    from kaggle_secrets import UserSecretsClient
    from cryptography.fernet import Fernet
except ImportError:
    print("警告: 关键库 (kaggle_secrets, cryptography) 未找到。解密功能将无法使用。")
    UserSecretsClient = None
    Fernet = None

# --- 第 1 步: 全局配置 ---

# -- A. MixFileCLI 配置 --
mixfile_config_yaml = """
uploader: "线路A2"
upload_task: 10
upload_retry: 10
download_task: 5
chunk_size: 1024
port: 4719
host: "0.0.0.0"
password: ""
webdav_path: "data.mix_dav"
history: "history.mix_list"
"""

# -- B. 加密的 FRP 服务配置 --
# !!! 重要 !!!
# 在这里粘贴你从 encrypt_util.py 工具中获得的加密配置字符串
ENCRYPTED_FRP_CONFIG = "gAAAAABotRuazfRzs7u7vMMCC5JblSb-ViSRTo0bRqRNINrHqMby9q2Um-8FLQiolwJUhZ3RMfNOFGzcyv45bbXmg3OyPZftGRMHMyDLkfsbc5A0ise_fEgbFx2IhEXOEDvfqzASuwqQy05UYHRn3QJE4i9Zrahv0-Z7KEMLBO02N81pAqzPU_w2wESz5E1vQh5kmiseU_t2"

# -- C. 本地服务配置 --
MIXFILE_LOCAL_PORT = 4719
FLASK_API_LOCAL_PORT = 5000
MIXFILE_REMOTE_PORT = 10001
FLASK_API_REMOTE_PORT = 10003

# --- 新增：解密模块 ---
def get_decrypted_frp_config():
    """从 Kaggle Secrets 获取密钥并解密 FRP 配置。"""
    if not UserSecretsClient or not Fernet:
        raise RuntimeError("无法执行解密：必要的库 (kaggle_secrets, cryptography) 未安装或导入失败。")

    print("🔐 正在从 Kaggle Secrets 获取解密密钥 'FRP_DECRYPTION_KEY'...")
    secrets = UserSecretsClient()
    try:
        decryption_key_password = secrets.get_secret("FRP_DECRYPTION_KEY")
    except Exception as e:
        print("❌ 无法从 Kaggle Secrets 获取密钥！请确保你已经正确设置了名为 'FRP_DECRYPTION_KEY' 的 Secret。")
        raise e

    # 从密码生成确定性的密钥 (必须与加密工具中的逻辑完全一致)
    key = base64.urlsafe_b64encode(hashlib.sha256(decryption_key_password.encode()).digest())
    cipher = Fernet(key)
    
    print("🔑 正在解密 FRP 配置...")
    try:
        decrypted_bytes = cipher.decrypt(ENCRYPTED_FRP_CONFIG.encode('utf-8'))
        config = json.loads(decrypted_bytes.decode('utf-8'))
        print("✅ FRP 配置解密成功！")
        return config
    except Exception as e:
        print("❌ 解密失败！这通常意味着你的 Kaggle Secret (密码) 与加密时使用的密码不匹配，或者加密字符串已损坏。")
        raise e

# --- 第 2 步: MixFileCLI Python 客户端 ---
class MixFileCLIClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
    
    def _make_request(self, method, url, **kwargs):
        try:
            headers = kwargs.get('headers', {})
            headers.update({'Cache-Control': 'no-cache', 'Pragma': 'no-cache', 'Expires': '0'})
            kwargs['headers'] = headers
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if e.response is not None:
                return (e.response.status_code, e.response.text)
            return (500, str(e))
            
    def upload_and_get_share_code(self, local_file_path, progress_callback=None):
        filename = os.path.basename(local_file_path)
        upload_url = urljoin(self.base_url, f"/api/upload/{quote(filename)}")
        file_size = os.path.getsize(local_file_path)
        
        def file_reader_generator(file_handle):
            chunk_size = 1024 * 1024
            bytes_read = 0
            while True:
                chunk = file_handle.read(chunk_size)
                if not chunk:
                    break
                bytes_read += len(chunk)
                if progress_callback:
                    progress_callback(bytes_read, file_size)
                yield chunk

        with open(local_file_path, 'rb') as f:
            return self._make_request("PUT", upload_url, data=file_reader_generator(f))

# --- 第 3 步: 混合模式 Flask API ---
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
tasks = {}
tasks_lock = threading.Lock()
api_client = None # 将在 main 函数中初始化

def process_upload_task(file_url, task_id=None, stream_yield_func=None):
    def update_status(status_data):
        if task_id:
            with tasks_lock:
                tasks[task_id].update(status_data)
        elif stream_yield_func:
            stream_yield_func(json.dumps(status_data, ensure_ascii=False) + "\n")
    try:
        filename_encoded = file_url.split("/")[-1].split("?")[0] or "downloaded_file"
        filename = unquote(filename_encoded)
        save_path = os.path.join("/kaggle/working/", filename)
        
        update_status({"status": "downloading", "stage": "download", "progress": 0, "details": "准备下载..."})
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            bytes_downloaded = 0
            last_update_time = 0
            update_interval = 0.5
            with open(save_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    current_time = time.time()
                    if current_time - last_update_time > update_interval:
                        progress = round((bytes_downloaded / total_size) * 100, 2) if total_size > 0 else 0
                        update_status({"status": "downloading", "stage": "download", "progress": progress, "details": f"已下载 {bytes_downloaded}/{total_size} 字节"})
                        last_update_time = current_time
        update_status({"status": "downloading", "stage": "download", "progress": 100, "details": f"已下载 {total_size}/{total_size} 字节"})
        
        def upload_callback(bytes_uploaded, total_bytes):
            progress = round((bytes_uploaded / total_bytes) * 100, 2) if total_bytes > 0 else 0
            update_status({"status": "uploading", "stage": "upload", "progress": progress, "details": f"已上传 {bytes_uploaded}/{total_bytes} 字节"})
        
        update_status({"status": "uploading", "stage": "upload", "progress": 0, "details": "准备上传并分享..."})
        
        upload_response = api_client.upload_and_get_share_code(save_path, progress_callback=upload_callback)
        os.remove(save_path)
        
        if isinstance(upload_response, requests.Response) and upload_response.ok:
            try:
                share_code = upload_response.text
                if not share_code or len(share_code) < 10:
                    raise ValueError("响应内容不是有效的分享码")
                
                result_data = {
                    "filename": filename,
                    "share_code": share_code,
                    "share_link": f"{api_client.base_url}/api/download/{quote(filename)}?s={share_code}"
                }
                update_status({"status": "success", "stage": "completed", "progress": 100, "details": "任务成功完成", "result": result_data})
            except (ValueError, json.JSONDecodeError):
                update_status({"status": "failed", "stage": "error", "details": f"上传成功但无法从响应中解析分享码: {upload_response.text}"})
        else:
            status_code, error_text = upload_response if isinstance(upload_response, tuple) else (upload_response.status_code, upload_response.text)
            update_status({"status": "failed", "stage": "error", "details": f"上传失败。状态码: {status_code}, 错误: {error_text}"})
    except Exception as e:
        update_status({"status": "failed", "stage": "error", "details": f"处理过程中发生严重错误: {str(e)}"})

@app.route("/api/upload", methods=["POST"])
def unified_upload():
    data = request.get_json()
    if not data or "url" not in data:
        return jsonify({"error": "请求体中必须包含 'url' 字段"}), 400
        
    mode = data.get("mode", "async")
    file_url = data["url"]
    
    if mode == "stream":
        def stream_generator():
            from queue import Queue
            q = Queue()
            threading.Thread(target=process_upload_task, args=(file_url, None, q.put)).start()
            while True:
                item = q.get()
                yield item
                try:
                    if json.loads(item).get('status') in ['success', 'failed', 'error']:
                        break
                except:
                    break
        return Response(stream_generator(), mimetype='application/x-ndjson')
        
    elif mode == "async":
        task_id = str(uuid.uuid4())
        with tasks_lock:
            tasks[task_id] = {"task_id": task_id, "status": "pending", "stage": "queue", "progress": 0, "details": "任务已创建"}
        threading.Thread(target=process_upload_task, args=(file_url, task_id, None)).start()
        return jsonify({"task_id": task_id, "status_url": f"/api/tasks/{task_id}"}), 202
        
    else:
        return jsonify({"error": f"不支持的模式: '{mode}'。请选择 'async' 或 'stream'。"}), 400

@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task_status(task_id):
    with tasks_lock:
        task = tasks.get(task_id)
    if task:
        return jsonify(task)
    else:
        return jsonify({"error": "未找到指定的 task_id"}), 404

# --- 第 4 步: 健壮的主程序 ---
def wait_for_port(port, host='127.0.0.1', timeout=60.0):
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                print(f"✅ 端口 {port} 已成功启动！")
                return True
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
            if time.perf_counter() - start_time >= timeout:
                print(f"❌ 等待端口 {port} 超时 ({timeout}秒)。")
                return False

def run_command(command, log_file=None):
    """通用函数，用于在后台运行 shell 命令并返回进程对象。"""
    print(f"🚀 正在执行: {command}")
    stdout_dest = None
    stderr_dest = None
    if log_file:
        log_handle = open(log_file, 'w')
        stdout_dest = log_handle
        stderr_dest = log_handle
    
    process = subprocess.Popen(command, shell=True, stdout=stdout_dest, stderr=stderr_dest)
    return process

def main():
    """主执行函数"""
    global api_client # 声明我们将修改全局的 api_client
    
    print(">>> 正在安装依赖库...");
    run_command("pip install -q flask requests cryptography").wait() # 等待安装完成
    
    try:
        # 1. 解密 FRP 配置
        frp_config = get_decrypted_frp_config()
        FRP_SERVER_ADDR = frp_config['FRP_SERVER_ADDR']
        FRP_SERVER_PORT = frp_config['FRP_SERVER_PORT']
        FRP_TOKEN = frp_config['FRP_TOKEN']
        
        # 2. 使用解密后的配置初始化 API Client
        api_client_base_url = f"http://{FRP_SERVER_ADDR}:{MIXFILE_REMOTE_PORT}"
        api_client = MixFileCLIClient(base_url=api_client_base_url)

        # 3. 启动所有服务
        print("\n>>> 正在创建自定义 config.yml...");
        with open("config.yml", "w") as f:
            f.write(mixfile_config_yaml)
        print("config.yml 文件已创建！")

        print("\n>>> 正在下载并启动 MixFileCLI...")
        run_command("wget -q --show-progress https://github.com/InvertGeek/mixfilecli/releases/download/2.0.1/mixfile-cli-2.0.1.jar").wait()
        java_command = "java -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -jar mixfile-cli-2.0.1.jar"
        run_command(java_command, "mixfile.log")
        
        if not wait_for_port(MIXFILE_LOCAL_PORT):
            print("❌ MixFileCLI 服务启动失败，请检查 mixfile.log 文件。程序将终止。")
            return

        def run_flask_app():
            app.run(host='0.0.0.0', port=FLASK_API_LOCAL_PORT, debug=False, use_reloader=False)
        
        print(f"\n>>> 正在启动 Flask API 服务...");
        threading.Thread(target=run_flask_app, daemon=True).start()
        time.sleep(5)
        print("✅ Flask API 服务已在后台启动。")

        print("\n>>> 正在启动 frpc 客户端...")
        if not os.path.exists("/kaggle/input/net-tools-new/frpc"):
            raise FileNotFoundError("❌ 错误: 未在 /kaggle/input/net-tools-new/ 目录下找到 frpc 文件。")
            
        run_command("cp /kaggle/input/net-tools-new/frpc /kaggle/working && chmod +x /kaggle/working/frpc").wait()
        
        frpc_ini_content = f"""
[common]
server_addr = {FRP_SERVER_ADDR}
server_port = {FRP_SERVER_PORT}
token = {FRP_TOKEN}
log_file = /kaggle/working/frpc_client.log

[mixfile_webdav_{MIXFILE_REMOTE_PORT}]
type = tcp
local_ip = 127.0.0.1
local_port = {MIXFILE_LOCAL_PORT}
remote_port = {MIXFILE_REMOTE_PORT}

[flask_api_{FLASK_API_REMOTE_PORT}]
type = tcp
local_ip = 127.0.0.1
local_port = {FLASK_API_LOCAL_PORT}
remote_port = {FLASK_API_REMOTE_PORT}
"""
        with open('./frp_combined.ini', 'w') as f:
            f.write(frpc_ini_content)
            
        run_command('/kaggle/working/frpc -c ./frp_combined.ini')
        time.sleep(5)
        print("✅ frpc 客户端已在后台启动。")

        print("\n" + "="*60)
        print("🎉 所有服务均已成功启动！您的混合模式 API 已上线。")
        print(f"  -> 上传任务地址 (POST): http://{FRP_SERVER_ADDR}:{FLASK_API_REMOTE_PORT}/api/upload")
        print(f"  -> 异步任务状态查询 (GET): http://{FRP_SERVER_ADDR}:{FLASK_API_REMOTE_PORT}/api/tasks/<task_id>")
        print("="*60)
        
        while True:
            time.sleep(300)
            print(f"🕒 服务持续运行中... ({time.ctime()})")
            
    except Exception as e:
        print(f"\n❌ 程序启动过程中发生致命错误: {e}")
    except KeyboardInterrupt:
        print("\n🛑 服务已手动停止。")

if __name__ == '__main__':
    main()