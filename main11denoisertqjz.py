# =============================================================================
#         Kaggle 持久化媒体处理服务 - 主应用 v2.1 (方案2 - 多进程隔离版)
# =============================================================================
#
# 功能:
#   - 启动 MixFileCLI 作为文件存储后端。
#   - 提供一个统一的 Flask API，用于:
#     1. 从 URL 下载文件并上传到 MixFile。
#     2. 从视频 URL 中提取高质量字幕 (可选)。
#     3. 将原始视频和生成的字幕灵活地上传到 MixFile。
#   - 通过 FRP 将内部服务安全地暴露到公网。
#   - 所有敏感配置 (FRP, API密钥) 均通过加密方式管理。
#   - 实现高容错的任务处理逻辑，子任务失败不影响整体流程。
#   - **V2.1 变更: 采用多进程架构，将重量级的媒体处理任务放到独立的子进程
#     中执行，以解决主进程中因服务环境冲突导致的AI模型加载失败问题。**
#
# =============================================================================

# --- 核心 Python 库 ---
import os
import subprocess
import threading
import time
import uuid
import socket
import json
import base64
import hashlib
import signal
import sys
import shutil
import mimetypes
from pathlib import Path
from datetime import timedelta
from urllib.parse import urljoin, quote, unquote
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, TimeoutError


# --- 多进程与队列 ---
import multiprocessing
from queue import Empty as QueueEmpty

# --- Web 框架与 HTTP 客户端 ---
from flask import Flask, request, jsonify, Response
import requests





# =============================================================================
# --- (新增模块) 第 3.5 步: V2Ray 代理管理器 ---
# =============================================================================
import base64
import json
import random
import string
from urllib.parse import urlparse, parse_qs




# --- 加密与密钥管理 ---
# 尝试导入关键库，如果失败则在后续检查中处理
try:
    from kaggle_secrets import UserSecretsClient
    from cryptography.fernet import Fernet
except ImportError:
    UserSecretsClient = None
    Fernet = None

# --- 字幕提取相关库 (将在 check_environment 中验证) ---
try:
    import torch
    import torchaudio
    from pydub import AudioSegment
    import pydantic
except ImportError as e:
    # 允许启动时导入失败，后续环境检查会捕获
    print(f"[警告] 预加载部分库失败: {e}。将在环境检查中确认。")
    torch = None
    torchaudio = None
    AudioSegment = None
    pydantic = None

# =============================================================================
# --- 第 1 步: 全局配置 ---
# =============================================================================

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

# -- B. 加密的服务配置 --
# !!! 重要 !!!
# 在这里粘贴你从 encrypt_util.py 工具中获得的加密配置字符串
# 第一个字符串用于 FRP (反向代理)
ENCRYPTED_FRP_CONFIG = "gAAAAABovqeXe55F4-o0c7vo8hIdPtcdQGWGAZganedI9sUqYQeyzDCzCxLLIjFLpCtTJQGedUh17nzhWhjgNEooN9ybgSYPnoHCSgKsGo64m0ghmUxPhbxrsocys4zki5IAMpuah_NrOH5YX4r-rQnKCI_S2yfurJp2E-eB_ciXodI8-X002KsMIK1ZpbvmVbH5I88bxV5V"

# 第二个字符串用于字幕服务 (Gemini API 密钥等)
ENCRYPTED_SUBTITLE_CONFIG = "gAAAAABovqfhTA9gwxUiOPivABQMk3PXitnzjVdCzOVTu9T1RcE7KSRQWHWqVn3geXJ--yA7IqD8Y3JIIG3X-zUA0j6xMwD9jOhysygys84C1pmm4LyqCg2FqEn-V6UitUxKzbOeXEMDPXVjSS_gP97-k6Dtmp38qSMp91v3BxNp94iJvcRNfDH4DbfZm4rrRwamVFeJzRiFGzPe35bwUqBXhUrIngQweiR1VTvtSnk0tnP53AfRdRPfuK9DSluhfASBrXQnt8bf9KtN0uzuc02SGMGtjFDt_nwxjfE2UEeC6nbQZ6peGDRbfQMFvEu8owsaWnaQdFe-YV7S4xjCjmmj9xw-2NnaFnplP3hYmPfLtE3EuvXXsQ4x30cnkwba0z_0-BEh0pXx-1kaR70T-uaDq5SHfAFbabIALekIRtaUX6EscuWxjU6MPP4S8Ld41kiub5mLVx5YzB3MAK7lMfC6gbtRQY9RWM-LZq739fXbf2U30W5M0R2ZDGsQNfAKcHoqfIQNPJBo" # 示例，请替换

# -- C. 本地服务与端口配置 --
MIXFILE_LOCAL_PORT = 4719
FLASK_API_LOCAL_PORT = 5000
MIXFILE_REMOTE_PORT = 20000  # 映射到公网的 MixFile 端口
FLASK_API_REMOTE_PORT = 20001  # 映射到公网的 Flask API 端口

# -- D. Killer API & 进程管理配置 --
KILLER_API_SHUTDOWN_TOKEN = "123456" # !!! 强烈建议修改 !!!
PROCESS_KEYWORDS_TO_KILL = ["java", "frpc", "python -c"] # 新增python子进程关键词
EXCLUDE_KEYWORDS_FROM_KILL = ["jupyter", "kernel", "ipykernel", "conda", "grep"]

# -- E. 字幕提取流程配置 --
# 这些值未来也可以加入到加密配置中
SUBTITLE_CHUNK_DURATION_MS = 10 * 60 * 1000  # 10分钟
SUBTITLE_BATCH_SIZE = 40
SUBTITLE_CONCURRENT_REQUESTS = 8
SUBTITLE_REQUESTS_PER_MINUTE = 8

# =============================================================================
# --- 第 2 步: 解密与配置加载模块 ---
# =============================================================================

class DecryptionError(Exception):
    # 自定义异常，用于表示解密过程中的特定失败。
    pass

def _get_decryption_cipher():
    #
    # 辅助函数：从 Kaggle Secrets 获取密码并生成 Fernet 解密器。
    # 这是一个共享逻辑，被其他解密函数调用。
    #
    if not UserSecretsClient or not Fernet:
        raise DecryptionError("关键库 (kaggle_secrets, cryptography) 未安装或导入失败。")
    
    print("🔐 正在从 Kaggle Secrets 获取解密密钥 'FRP_DECRYPTION_KEY'...")
    try:
        secrets = UserSecretsClient()
        decryption_key_password = secrets.get_secret("FRP_DECRYPTION_KEY")
        if not decryption_key_password:
             raise ValueError("Kaggle Secret 'FRP_DECRYPTION_KEY' 的值为空。")
    except Exception as e:
        print(f"❌ 无法从 Kaggle Secrets 获取密钥！请确保你已正确设置了名为 'FRP_DECRYPTION_KEY' 的 Secret。")
        raise DecryptionError(f"获取 Kaggle Secret 失败: {e}") from e

    # 使用 SHA256 从用户密码派生一个确定性的、安全的32字节密钥
    key = base64.urlsafe_b64encode(hashlib.sha256(decryption_key_password.encode()).digest())
    return Fernet(key)

def get_decrypted_config(encrypted_string: str, config_name: str) -> dict:
    #
    # 通用的解密函数，用于解密任何给定的加密字符串。
    #
    # Args:
    #     encrypted_string (str): 从 encrypt_util.py 获取的 Base64 编码的加密字符串。
    #     config_name (str): 配置的名称，用于日志输出 (例如 "FRP", "Subtitle")。
    #
    # Returns:
    #     dict: 解密并解析后的配置字典。
    # 
    # Raises:
    #     DecryptionError: 如果解密过程的任何步骤失败。
    #
    print(f"🔑 正在准备解密 {config_name} 配置...")
    try:
        if "PASTE_YOUR_ENCRYPTED" in encrypted_string:
             raise ValueError(f"检测到占位符加密字符串，请替换为你的真实配置。")
        
        cipher = _get_decryption_cipher()
        decrypted_bytes = cipher.decrypt(encrypted_string.encode('utf-8'))
        config = json.loads(decrypted_bytes.decode('utf-8'))
        print(f"✅ {config_name} 配置解密成功！")
        return config
    except ValueError as e:
        print(f"❌ 解密 {config_name} 配置失败: {e}")
        raise DecryptionError(f"配置字符串无效: {e}")
    except Exception as e:
        print(f"❌ 解密 {config_name} 配置失败！")
        print("   这通常意味着你的 Kaggle Secret (密码) 与加密时使用的密码不匹配，")
        print("   或者加密字符串已损坏。")
        raise DecryptionError(f"解密过程中发生未知错误: {e}") from e
        
# =============================================================================
# --- 第 3 步: 环境检查、辅助函数与 Killer API ---
# =============================================================================

def check_environment():
    #
    # 检查运行所需的所有关键依赖和库是否都已正确安装。
    # 如果缺少关键组件，则会抛出异常，使程序提前失败。
    #
    print("\n" + "="*60)
    print("🔬 正在执行环境依赖检查...")
    print("="*60)
    
    # 检查基础库
    if not UserSecretsClient or not Fernet:
        raise RuntimeError("关键安全库 'kaggle_secrets' 或 'cryptography' 未找到。无法继续。")
    print("✅ 安全库 (kaggle_secrets, cryptography) - 正常")
    
    # 检查字幕提取核心库
    critical_subtitle_libs = {
        "torch": torch,
        "torchaudio": torchaudio,
        "pydub": AudioSegment,
        "pydantic": pydantic
    }
    for name, lib in critical_subtitle_libs.items():
        if not lib:
            raise RuntimeError(f"字幕提取核心库 '{name}' 未找到或导入失败。请检查 Kaggle 环境。")
        print(f"✅ 字幕库 ({name}) - 正常")

    # 检查命令行工具
    if not shutil.which("ffmpeg"):
        raise RuntimeError("'ffmpeg' 命令未找到。这是提取音频所必需的。")
    print("✅ 命令行工具 (ffmpeg) - 正常")
    
    if not shutil.which("java"):
        raise RuntimeError("'java' 命令未找到。这是运行 MixFileCLI 所必需的。")
    print("✅ 命令行工具 (java) - 正常")

    print("\n✅ 环境检查通过！所有关键依赖均已就绪。\n")


def log_system_event(level: str, message: str, in_worker=False):
    # 一个简单的带时间戳的日志记录器。
    # 新增 in_worker 参数以区分日志来源
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    worker_tag = "[WORKER]" if in_worker else "[SYSTEM]"
    prefix = f"[{timestamp} {worker_tag} {level.upper()}]"
    print(f"{prefix} {message}", flush=True)


def ms_to_srt_time(ms: int) -> str:
    # 将毫秒转换为 SRT 字幕文件的时间格式 (HH:MM:SS,ms)。
    try:
        ms = int(ms)
    except (ValueError, TypeError):
        ms = 0
    td = timedelta(milliseconds=ms)
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = td.microseconds // 1000
    return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"


def run_command(command: str, log_file: str = None) -> subprocess.Popen:
    #
    # 在后台以非阻塞方式执行一个 shell 命令。
    #
    # Args:
    #     command (str): 要执行的命令。
    #     log_file (str, optional): 将 stdout 和 stderr 重定向到的日志文件名。
    #
    # Returns:
    #     subprocess.Popen: 正在运行的进程对象。
    #
    log_system_event("info", f"执行命令: {command}")
    stdout_dest = None
    stderr_dest = None
    if log_file:
        log_handle = open(log_file, 'w', encoding='utf-8')
        stdout_dest = log_handle
        stderr_dest = log_handle
    
    # 使用 Popen 以非阻塞方式启动进程
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=stdout_dest,
        stderr=stderr_dest,
        encoding='utf-8',
        errors='ignore'
    )
    return process


def wait_for_port(port: int, host: str = '127.0.0.1', timeout: float = 60.0) -> bool:
    #
    # 等待指定的本地端口变为可用状态。
    #
    # Args:
    #     port (int): 要检查的端口号。
    #     host (str, optional): 监听的主机地址。默认为 '127.0.0.1'。
    #     timeout (float, optional): 最大等待时间（秒）。默认为 60.0。
    #
    # Returns:
    #     bool: 如果端口在超时前变为可用，则返回 True，否则返回 False。
    #
    log_system_event("info", f"正在等待端口 {host}:{port} 启动...")
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                log_system_event("info", f"✅ 端口 {port} 已成功启动！")
                return True
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(1)
            if time.perf_counter() - start_time >= timeout:
                log_system_event("error", f"❌ 等待端口 {port} 超时 ({timeout}秒)。")
                return False

# --- Killer API 逻辑 (用于远程关闭) ---

def _find_and_kill_targeted_processes(signal_to_send=signal.SIGTERM):
    # 查找并终止此脚本启动的所有关键子进程 (java, frpc, python -c ...)。
    killed_pids_info = []
    log_system_event("info", f"查找并尝试终止与 '{PROCESS_KEYWORDS_TO_KILL}' 相关的进程...")
    
    current_kernel_pid = os.getpid()
    pids_to_target = set()

    try:
        # 使用 ps 命令获取所有进程信息
        ps_output = subprocess.check_output(['ps', '-eo', 'pid,ppid,args'], text=True)
        
        for line in ps_output.splitlines()[1:]:
            parts = line.strip().split(None, 2)
            if len(parts) < 3: continue
            
            try:
                pid = int(parts[0])
                command_line = parts[2]
                command_lower = command_line.lower()
            except (ValueError, IndexError):
                continue
            
            # 排除当前内核进程和系统关键进程
            if pid == current_kernel_pid: continue
            is_excluded = any(ex_kw.lower() in command_lower for ex_kw in EXCLUDE_KEYWORDS_FROM_KILL)
            if is_excluded: continue
            
            # 检查是否为目标进程
            is_target = any(target_kw.lower() in command_lower for target_kw in PROCESS_KEYWORDS_TO_KILL)
            if is_target:
                pids_to_target.add(pid)
                log_system_event("debug", f"  发现目标: PID={pid}, CMD='{command_line}'")

        if not pids_to_target:
            log_system_event("info", "未找到需要终止的特定应用子进程。")
        else:
            log_system_event("info", f"准备向 PIDs {list(pids_to_target)} 发送信号 {signal_to_send}...")
            for pid_to_kill in pids_to_target:
                try:
                    os.kill(pid_to_kill, signal_to_send)
                    log_system_event("info", f"    已向 PID {pid_to_kill} 发送信号 {signal_to_send}.")
                    killed_pids_info.append({"pid": pid_to_kill, "status": "signal_sent"})
                except ProcessLookupError:
                    killed_pids_info.append({"pid": pid_to_kill, "status": "not_found"})
                except Exception as e:
                    killed_pids_info.append({"pid": pid_to_kill, "status": f"error_{type(e).__name__}"})
                    
    except Exception as e:
        log_system_event("error", f"查找或终止子进程时出错: {e}")
    
    return killed_pids_info


def _shutdown_notebook_kernel_immediately():
    #
    # 通过发送 SIGKILL 信号强制、立即地关闭当前 Kaggle Notebook 内核。
    # 这是最终的自毁指令。
    #
    log_system_event("critical", "准备通过 SIGKILL 信号强制关闭当前 Kaggle Notebook Kernel...")
    sys.stdout.flush()
    sys.stderr.flush()
    time.sleep(0.5) # 留出一点时间让日志刷新
    
    # SIGKILL (信号 9) 是一个无法被捕获或忽略的信号，比 os._exit 更为强制。
    os.kill(os.getpid(), signal.SIGKILL)



# =============================================================================
# --- (新增模块) 第 3.5 步: V2Ray 代理管理器 ---
# =============================================================================

class ProxyManager:
    """
    负责下载、测试和管理 V2Ray/Xray 代理客户端，以加速文件上传。
    """
    def __init__(self, sub_url, mixfile_base_url):
        self.sub_url = sub_url
        self.mixfile_base_url = mixfile_base_url
        self.v2ray_path = Path("/kaggle/working/xray")
        self.config_path = Path("/kaggle/working/xray_config.json")
        self.local_socks_port = 10808
        self.best_node_config = None
        self.best_node_speed = 0  # in MB/s

    def _download_xray(self):
        """【依赖修复版】下载 Xray 核心的同时，也下载其路由所需的 geoip.dat 和 geosite.dat 文件。"""
        geoip_path = Path("/kaggle/working/geoip.dat")
        geosite_path = Path("/kaggle/working/geosite.dat")

        # 检查核心和数据库文件是否都存在
        if self.v2ray_path.exists() and geoip_path.exists() and geosite_path.exists():
            log_system_event("info", "Xray 核心及数据库文件已存在，跳过下载。")
            return
        
        log_system_event("info", "正在下载 Xray 核心及数据库文件...")
        
        # 定义下载URL
        xray_url = "https://github.com/XTLS/Xray-core/releases/download/v1.8.10/Xray-linux-64.zip"
        geoip_url = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"
        geosite_url = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat"
        
        zip_path = Path("/kaggle/working/xray.zip")
        
        try:
            # 下载 Xray 核心
            if not self.v2ray_path.exists():
                log_system_event("info", "  -> 下载 Xray core...")
                run_command(f"wget -q -O {zip_path} {xray_url}").wait()
                run_command(f"unzip -o {zip_path} xray -d /kaggle/working/").wait()
                self.v2ray_path.chmod(0o755)
                zip_path.unlink()

            # 下载 geoip.dat
            if not geoip_path.exists():
                log_system_event("info", "  -> 下载 geoip.dat...")
                run_command(f"wget -q -O {geoip_path} {geoip_url}").wait()
            
            # 下载 geosite.dat
            if not geosite_path.exists():
                log_system_event("info", "  -> 下载 geosite.dat...")
                run_command(f"wget -q -O {geosite_path} {geosite_url}").wait()

            log_system_event("info", "✅ Xray 核心及数据库文件下载完成。")
        except Exception as e:
            raise RuntimeError(f"下载 Xray 组件失败: {e}")

    def _fetch_and_parse_subscription(self):
        """获取并解析订阅链接，返回节点链接列表。"""
        log_system_event("info", f"正在从 {self.sub_url[:30]}... 获取订阅...")
        try:
            response = requests.get(self.sub_url, timeout=20)
            response.raise_for_status()
            decoded_content = base64.b64decode(response.content).decode('utf-8')
            return decoded_content.strip().split('\n')
        except Exception as e:
            log_system_event("error", f"获取或解析订阅失败: {e}")
            return []

    def _generate_node_config(self, node_url):
        """【路由修复版】在配置中加入智能路由，区分代理流量和直连流量。"""
        try:
            parsed_url = urlparse(node_url)
            node_name_raw = parsed_url.fragment
            node_name = unquote(node_name_raw) if node_name_raw else "Unnamed Node"
            
            # --- 步骤 1: 构建 Outbounds ---
            #
            # Proxy Outbound (tag: "proxy")
            protocol = parsed_url.scheme
            proxy_outbound = {"protocol": protocol, "settings": {}, "tag": "proxy"}
            
            if protocol == "vmess":
                # ... vmess 逻辑 (保持不变) ...
                try:
                    decoded_vmess_str = base64.b64decode(parsed_url.netloc).decode('utf-8')
                    decoded_vmess = json.loads(decoded_vmess_str)
                except Exception:
                     return None, f"Invalid VMess format for node {node_name}"

                proxy_outbound["settings"]["vnext"] = [{
                    "address": decoded_vmess["add"],
                    "port": int(decoded_vmess["port"]),
                    "users": [{"id": decoded_vmess["id"], "alterId": int(decoded_vmess["aid"]), "security": decoded_vmess.get("scy", "auto")}]
                }]
                stream_settings = {"network": decoded_vmess.get("net", "tcp")}
                if stream_settings["network"] == "ws":
                    ws_settings = {"path": decoded_vmess.get("path", "/")}
                    host = decoded_vmess.get("host")
                    if host:
                        ws_settings["headers"] = {"Host": host}
                    stream_settings["wsSettings"] = ws_settings
                if decoded_vmess.get("tls", "") == "tls":
                     stream_settings["security"] = "tls"
                     stream_settings["tlsSettings"] = {"serverName": decoded_vmess.get("sni", decoded_vmess.get("host", decoded_vmess["add"]))}
                proxy_outbound["streamSettings"] = stream_settings

            elif protocol == "vless":
                # ... vless 逻辑 (保持不变) ...
                qs = parse_qs(parsed_url.query)
                user_obj = { "id": parsed_url.username, "encryption": "none", "flow": qs.get("flow", [None])[0], "alterId": 0, "security": "auto" }
                if user_obj["flow"] is None: del user_obj["flow"]
                proxy_outbound["settings"]["vnext"] = [{"address": parsed_url.hostname, "port": int(parsed_url.port), "users": [user_obj]}]
                stream_settings = {"network": qs.get("type", ["tcp"])[0]}
                if stream_settings["network"] == "ws":
                    ws_path = unquote(qs.get("path", ["/"])[0])
                    ws_host = unquote(qs.get("host", [""])[0])
                    ws_settings = {"path": ws_path}
                    if ws_host: ws_settings["headers"] = {"Host": ws_host}
                    stream_settings["wsSettings"] = ws_settings
                if qs.get("security", ["none"])[0] == "tls":
                    stream_settings["security"] = "tls"
                    tls_sni = unquote(qs.get("sni", [""])[0]) or unquote(qs.get("host", [""])[0]) or parsed_url.hostname
                    fp = qs.get("fp", ["random"])[0]
                    tls_settings = {"serverName": tls_sni, "fingerprint": fp, "allowInsecure": False, "show": False}
                    alpn = qs.get("alpn")
                    if alpn: tls_settings["alpn"] = [val for val in alpn[0].split(',') if val]
                    stream_settings["tlsSettings"] = tls_settings
                proxy_outbound["streamSettings"] = stream_settings

            else:
                return None, f"Unsupported protocol: {protocol}"

            # Direct Outbound (tag: "direct")
            direct_outbound = {"protocol": "freedom", "settings": {}, "tag": "direct"}
            
            # Block Outbound (tag: "block") - for ads, etc.
            block_outbound = {"protocol": "blackhole", "settings": {}, "tag": "block"}

            # --- 步骤 2: 构建最终配置 ---
            config = {
                "log": {"loglevel": "warning"},
                "dns": { "servers": ["8.8.8.8", "1.1.1.1", "localhost"] },
                "inbounds": [{
                    "port": self.local_socks_port,
                    "protocol": "socks",
                    "listen": "127.0.0.1",
                    "settings": {"auth": "noauth", "udp": True},
                    "sniffing": { "enabled": True, "destOverride": ["http", "tls"] }
                }],
                "outbounds": [
                    proxy_outbound,
                    direct_outbound,
                    block_outbound
                ],
                "routing": {
                    "domainStrategy": "AsIs",
                    "rules": [
                        { # 规则1: 直连私有地址和本地地址
                            "type": "field",
                            "ip": ["geoip:private"],
                            "outboundTag": "direct"
                        },
                        { # 规则2: (可选) 拦截广告
                            "type": "field",
                            "domain": ["geosite:category-ads-all"],
                            "outboundTag": "block"
                        },
                        # 默认规则：其他所有流量都走代理
                        # （Xray默认会将不匹配任何规则的流量发往第一个outbound，即proxy_outbound）
                    ]
                }
            }
            
            return config, node_name
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, f"Error parsing node '{node_name}': {e}"

    def _test_node_upload_speed(self, node_config, node_name):
        """启动节点并测试其上传速度，返回MB/s。"""
        log_system_event("info", f"  -> 正在测试节点: {node_name}...")
        with open(self.config_path, 'w') as f:
            json.dump(node_config, f)
        
        process = run_command(f"{self.v2ray_path} -c {self.config_path}")
        if not wait_for_port(self.local_socks_port, timeout=10):
            log_system_event("warning", f"     节点 {node_name} 启动失败。")
            process.terminate()
            process.wait()
            return 0

        speed = 0
        try:
            proxies = {
                'http': f'socks5h://127.0.0.1:{self.local_socks_port}',
                'https': f'socks5h://127.0.0.1:{self.local_socks_port}',
            }
            # 创建一个 2MB 的随机文件用于测试
            test_data_size = 5 * 1024 * 1024
            test_data = ''.join(random.choices(string.ascii_letters + string.digits, k=test_data_size)).encode()
            
            test_upload_url = urljoin(self.mixfile_base_url, "/api/upload/proxy_test.tmp")
            
            start_time = time.time()
            # 注意：这里的测试目标是MixFile服务，确保测试的是真实上传链路
            response = requests.put(test_upload_url, data=test_data, proxies=proxies, timeout=60)
            end_time = time.time()
            
            response.raise_for_status()
            
            duration = end_time - start_time
            speed = (test_data_size / duration) / (1024 * 1024) # MB/s
            log_system_event("info", f"     ✅ 节点 {node_name} 可用，上传速度: {speed:.2f} MB/s")
        except Exception as e:
            log_system_event("warning", f"     ❌ 节点 {node_name} 测试失败: {e}")
        finally:
            process.terminate()
            process.wait()
            time.sleep(1) # 确保端口已释放
        return speed

    def setup_best_proxy(self):
        """主流程：寻找并启动最快的代理节点。"""
        global GLOBAL_PROXY_SETTINGS
        try:
            self._download_xray()
            node_urls = self._fetch_and_parse_subscription()
            if not node_urls:
                log_system_event("warning", "未获取到任何代理节点，将不使用代理。")
                return

            log_system_event("info", f"获取到 {len(node_urls)} 个节点，开始测速...")
            
            for node_url in node_urls:
                node_config, node_name = self._generate_node_config(node_url.strip())
                if not node_config:
                    log_system_event("debug", f"跳过不支持的节点或解析失败: {node_name}")
                    continue
                
                speed = self._test_node_upload_speed(node_config, node_name)
                if speed > self.best_node_speed:
                    self.best_node_speed = speed
                    self.best_node_config = node_config
            
            if self.best_node_config:
                log_system_event("info", "="*60)
                log_system_event("info", f"🚀 最快节点选择完成！速度: {self.best_node_speed:.2f} MB/s")
                log_system_event("info", "正在后台启动此节点用于后续所有上传任务...")
                log_system_event("info", "="*60)

                with open(self.config_path, 'w') as f:
                    json.dump(self.best_node_config, f)
                
                run_command(f"{self.v2ray_path} -c {self.config_path}", "xray.log")
                if not wait_for_port(self.local_socks_port, timeout=10):
                    raise RuntimeError("启动最优代理节点失败！")
                
                GLOBAL_PROXY_SETTINGS = {
                    'http': f'socks5h://127.0.0.1:{self.local_socks_port}',
                    'https': f'socks5h://127.0.0.1:{self.local_socks_port}',
                }
            else:
                log_system_event("warning", "所有节点均测试失败，本次运行将不使用代理。")

        except Exception as e:
            log_system_event("error", f"设置代理时发生严重错误: {e}。将不使用代理。")

# =============================================================================
# --- 第 4 步: 字幕提取核心模块 (在子进程中调用) ---
# =============================================================================

# --- A. Pydantic 数据验证模型 ---
# 用于严格验证 Gemini API 返回的 JSON 结构，确保数据质量。

class SubtitleLine(pydantic.BaseModel):
    start_ms: int
    end_ms: int | None = None
    text: str

class BatchTranscriptionResult(pydantic.BaseModel):
    subtitles: list[SubtitleLine]

def load_ai_models():
    """
    在工作进程启动时预加载所有需要的 AI 模型。
    返回一个包含已加载模型的字典。
    """
    log_system_event("info", "工作进程正在预加载 AI 模型...", in_worker=True)
    loaded_models = {
        "denoiser": None
    }
    
    # 1. 尝试加载 AI 降噪模型
    try:
        from denoiser import pretrained
        log_system_event("info", "正在加载 AI 降噪模型 (denoiser)...", in_worker=True)
        denoiser_model = pretrained.dns64().cuda()
        loaded_models["denoiser"] = denoiser_model
        log_system_event("info", "✅ AI 降噪模型加载成功。", in_worker=True)
    except Exception as e:
        log_system_event("warning", f"预加载 AI 降噪模型失败，降噪功能将不可用。错误: {e}", in_worker=True)

    # 2. VAD 模型说明
    # faster-whisper 的 VAD 功能 (get_speech_timestamps) 是一个轻量级函数，
    # 不需要像降噪模型那样进行重量级的预加载。因此，我们在这里只加载降噪模型。
    
    return loaded_models

# --- B. 动态提示词与文件处理 ---

def get_dynamic_prompts(api_url: str) -> tuple[str, str]:
    #
    # 从指定的 API 获取动态提示词。如果失败，则返回硬编码的备用提示词。
    #
    log_system_event("info", "正在尝试从 API 获取动态提示词...", in_worker=True)
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        prompts = response.json()
        if "system_instruction" in prompts and "prompt_for_task" in prompts:
            log_system_event("info", "✅ 成功从 API 获取动态提示词。", in_worker=True)
            return prompts['system_instruction'], prompts['prompt_for_task']
        else:
            raise ValueError("API 响应中缺少必要的键。")
    except Exception as e:
        log_system_event("warning", f"获取动态提示词失败: {e}。将使用备用提示词。", in_worker=True)
        # --- 备用提示词 (Fallback Prompts) ---
        fallback_system = (
            "你是一个专业的字幕翻译模型。你的唯一任务是将用户提供的任何语言的音频内容翻译成**简体中文**。"
            "在你的所有输出中，`text` 字段的值**必须是简体中文**。"
            "**绝对禁止**在 `text` 字段中返回任何原始语言（如日语）的文本。"
        )
        fallback_task = (
            "我将为你提供一系列音频片段和它们在视频中的绝对时间 `[AUDIO_INFO] start_ms --> end_ms`。\n"
            "请执行以下操作：\n"
            "1.  **听取音频**并理解其内容。\n"
            "2.  **创建内部时间轴**: 将**翻译成中文后**的文本，根据语音停顿分割成更短的字幕行，并为每一行估算一个精确的 `start_ms` 和 `end_ms`。\n"
            "3.  **格式化输出**: 你的输出必须是一个格式完全正确的 JSON 对象，不要包含任何 markdown 标记。结构如下，其中 `text` 字段的值必须是你翻译后的**简体中文**：\n"
            '{\n'
            '  "subtitles": [\n'
            '    { "start_ms": 12345, "end_ms": 13456, "text": "这是翻译后的第一句字幕" },\n'
            '    { "start_ms": 13600, "text": "这是第二句" }\n'
            '  ]\n'
            '}\n'
        )
        return fallback_system, fallback_task


def read_and_encode_file_base64(filepath: str) -> str | None:
    # 读取文件内容并返回 Base64 编码的字符串。
    try:
        with open(filepath, "rb") as f:
            binary_data = f.read()
        return base64.b64encode(binary_data).decode('utf-8')
    except Exception as e:
        log_system_event("error", f"无法读取或编码文件: {filepath}. 错误: {e}", in_worker=True)
        return None

# --- C. 音频预处理管道 ---

def preprocess_audio_for_subtitles(
    video_path: Path,
    temp_dir: Path,
    update_status_callback: callable,
    ai_models: dict  # <--- 修改：增加 ai_models 参数
) -> list[dict]:
    #
    # 完整的音频预处理流程：提取 -> 降噪 -> VAD 切分。
    #
    # Args:
    #     video_path (Path): 输入的视频文件路径。
    #     temp_dir (Path): 用于存放所有中间文件的临时目录。
    #     update_status_callback (callable): 用于更新任务状态的回调函数。
    #
    # Returns:
    #     list[dict]: 一个包含所有有效语音片段信息的列表，
    #                 每个元素是 {"path": str, "start_ms": int, "end_ms": int}。
    # 
    # Raises:
    #     Exception: 如果在任何关键步骤（如 ffmpeg）中发生失败。
    #
    
    # 1. 使用 ffmpeg 提取原始音频
    update_status_callback(stage="subtitle_extract_audio", details="正在从视频中提取音频...")
    raw_audio_path = temp_dir / "raw_audio.wav"
    try:
        command = [
            "ffmpeg", "-i", str(video_path),
            "-ac", "1", "-ar", "16000", # 单声道, 16kHz 采样率 (语音识别标准)
            "-vn", "-y", "-loglevel", "error", str(raw_audio_path)
        ]
        # 使用 subprocess.run 等待命令完成
        process = subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        log_system_event("error", f"FFmpeg 提取音频失败。Stderr: {e.stderr}", in_worker=True)
        raise RuntimeError(f"FFmpeg 提取音频失败: {e.stderr}")

    # 2. 尝试加载 AI 降噪模型
    # 2. 获取预加载的 AI 降噪模型
    denoiser_model = ai_models.get("denoiser")
    if denoiser_model:
        update_status_callback(stage="subtitle_denoise", details="AI 降噪模型已加载，准备处理...")
        log_system_event("info", "将使用预加载的 AI 降噪模型。", in_worker=True)
    else:
        log_system_event("warning", "降噪模型不可用，将跳过降噪步骤。", in_worker=True)
    # <--- 修改结束 --->


    # 3. 分块处理音频：降噪 -> VAD
    update_status_callback(stage="subtitle_vad", details="正在进行音频分块与语音检测...")
    original_audio = AudioSegment.from_wav(raw_audio_path)
    total_duration_ms = len(original_audio)
    chunk_files = []
    chunks_dir = temp_dir / "audio_chunks"
    chunks_dir.mkdir(exist_ok=True)
    num_chunks = -(-total_duration_ms // SUBTITLE_CHUNK_DURATION_MS)

    for i in range(num_chunks):
        start_time_ms = i * SUBTITLE_CHUNK_DURATION_MS
        end_time_ms = min((i + 1) * SUBTITLE_CHUNK_DURATION_MS, total_duration_ms)
        
        log_system_event("info", f"正在处理音频总块 {i+1}/{num_chunks}...", in_worker=True)
        
        audio_chunk = original_audio[start_time_ms:end_time_ms]
        temp_chunk_path = temp_dir / f"temp_chunk_{i}.wav"
        audio_chunk.export(temp_chunk_path, format="wav")
        
        processing_path = temp_chunk_path
        
        # 3.1 AI 降噪 (如果模型加载成功)
        if denoiser_model:
            try:
                wav, sr = torchaudio.load(temp_chunk_path)
                wav = wav.cuda()
                with torch.no_grad():
                    denoised_wav = denoiser_model(wav[None])[0]
                denoised_chunk_path = temp_dir / f"denoised_chunk_{i}.wav"
                torchaudio.save(denoised_chunk_path, denoised_wav.cpu(), 16000)
                processing_path = denoised_chunk_path
            except Exception as e:
                log_system_event("warning", f"当前块降噪失败，将使用原始音频。错误: {e}", in_worker=True)
        
        # 3.2 VAD 语音检测
        try:
            from faster_whisper.audio import decode_audio
            from faster_whisper.vad import VadOptions, get_speech_timestamps
            
            vad_parameters = {
                "threshold": 0.38, 
                "min_speech_duration_ms": 150, 
                "max_speech_duration_s": 15.0, # 稍微放宽以容纳长句
                "min_silence_duration_ms": 1500, 
                "speech_pad_ms": 500
            }
            sampling_rate = 16000
            audio_data = decode_audio(str(processing_path), sampling_rate=sampling_rate)
            speech_timestamps = get_speech_timestamps(audio_data, vad_options=VadOptions(**vad_parameters))
            
            # 3.3 根据 VAD 结果从原始音频中精确切片
            for speech in speech_timestamps:
                relative_start_ms = int(speech["start"] / sampling_rate * 1000)
                relative_end_ms = int(speech["end"] / sampling_rate * 1000)
                absolute_start_ms = start_time_ms + relative_start_ms
                absolute_end_ms = start_time_ms + relative_end_ms
                
                final_chunk = original_audio[absolute_start_ms:absolute_end_ms]
                final_chunk_path = chunks_dir / f"chunk_{absolute_start_ms}.wav"
                final_chunk.export(str(final_chunk_path), format="wav")
                chunk_files.append({
                    "path": str(final_chunk_path),
                    "start_ms": absolute_start_ms,
                    "end_ms": absolute_end_ms
                })
        except Exception as e:
            log_system_event("error", f"当前块 VAD 处理失败: {e}", in_worker=True)
    
    log_system_event("info", f"音频分块处理完成，总共切分为 {len(chunk_files)} 个有效语音片段。", in_worker=True)
    return chunk_files

# --- D. AI 交互与并发调度 ---

def _process_subtitle_batch_with_ai(
    chunk_group: list[dict],
    group_index: int,
    api_key: str,
    gemini_endpoint_prefix: str,
    prompt_api_url: str
) -> list[dict]:
    """
    处理单个批次的音频块，调用 Gemini API 获取字幕。
    (优化版 v2: 明确抛出异常，而不是返回空列表，以便上层捕获)
    """
    log_system_event("info", f"[字幕任务 {group_index+1}] 已在独立进程中启动...", in_worker=True)
    
    # 【核心修改】: 将整个函数逻辑包裹在一个大的 try...except 块中。
    # 这样做的目的是，无论发生何种类型的错误（网络、API、数据解析等），
    # 都能确保它被重新抛出，而不是被函数内部消化掉。
    try:
        # 1. 获取动态提示词
        system_instruction, prompt_for_task = get_dynamic_prompts(prompt_api_url)

        # 2. 构建 REST API 请求体 (payload)
        model_name = "gemini-2.5-flash"
        generate_url = f"{gemini_endpoint_prefix.rstrip('/')}/v1beta/models/{model_name}:generateContent"
        headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
        
        parts = [{"text": prompt_for_task}]
        for chunk in chunk_group:
            encoded_data = read_and_encode_file_base64(chunk["path"])
            if not encoded_data:
                # 如果单个文件编码失败，记录警告并跳过，不影响整个批次
                log_system_event("warning", f"[字幕任务 {group_index+1}] 文件 {chunk['path']} 编码失败，将跳过此音频片段。", in_worker=True)
                continue
            parts.append({"text": f"[AUDIO_INFO] {chunk['start_ms']} --> {chunk['end_ms']}"})
            parts.append({"inlineData": {"mime_type": "audio/wav", "data": encoded_data}})
        
        if len(parts) <= 1:
            # 如果整个批次的所有文件都无法编码，这是一个致命错误，应抛出异常
            raise ValueError(f"批次 {group_index+1} 中的所有音频文件均无法编码，任务中止。")

        payload = {
            "contents": [{"parts": parts}],
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "safetySettings": [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
            ],
            "generationConfig": {"responseMimeType": "application/json"}
        }

        # 3. 发送请求并实现全面的重试逻辑
        log_system_event("info", f"[字幕任务 {group_index+1}] 数据准备完毕，正在调用 Gemini API...", in_worker=True)
        max_retries = 3
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # 使用稍长的超时时间，因为可能包含大量音频数据
                response = requests.post(generate_url, headers=headers, json=payload, timeout=300) 
                
                # 可重试的服务器端错误
                if response.status_code in [429, 500, 503, 504]:
                    log_system_event("warning", f"[字幕任务 {group_index+1}] 遇到可重试的 API 错误 (HTTP {response.status_code}, 尝试 {attempt + 1}/{max_retries})。", in_worker=True)
                    # 触发下一次循环的重试
                    raise requests.exceptions.HTTPError(f"Server error: {response.status_code}")

                # 任何其他非2xx的状态码，都视为不可重试的失败
                response.raise_for_status()
                
                # --- 如果请求成功，则处理响应 ---
                response_data = response.json()
                
                candidates = response_data.get("candidates")
                if not candidates:
                    raise ValueError(f"API响应中缺少 'candidates' 字段。响应: {response.text}")

                content = candidates[0].get("content")
                if not content:
                    finish_reason = candidates[0].get("finishReason", "未知")
                    safety_ratings = candidates[0].get("safetyRatings", [])
                    raise ValueError(f"API响应内容为空(可能被安全策略拦截)。原因: {finish_reason}, 安全评级: {safety_ratings}")

                json_text = content.get("parts", [{}])[0].get("text")
                if json_text is None:
                    raise ValueError(f"API响应的 'parts' 中缺少 'text' 字段。响应: {response.text}")

                # 使用 Pydantic 验证 JSON 结构
                parsed_result = BatchTranscriptionResult.model_validate_json(json_text)
                subtitles_count = len(parsed_result.subtitles)
                log_system_event("info", f"✅ [字幕任务 {group_index+1}] 成功！获得 {subtitles_count} 条字幕。", in_worker=True)
                
                thread_local_srt_list = []
                for i, subtitle in enumerate(parsed_result.subtitles):
                    if subtitle.end_ms is None:
                        if i + 1 < subtitles_count:
                            subtitle.end_ms = parsed_result.subtitles[i+1].start_ms
                        else:
                            subtitle.end_ms = chunk_group[-1]['end_ms']
                    if subtitle.start_ms > subtitle.end_ms:
                        subtitle.end_ms = subtitle.start_ms + 250
                    
                    line = f"{ms_to_srt_time(subtitle.start_ms)} --> {ms_to_srt_time(subtitle.end_ms)}\n{subtitle.text.strip()}\n"
                    thread_local_srt_list.append({"start_ms": subtitle.start_ms, "srt_line": line})
                
                # 成功处理，返回结果，函数结束
                return thread_local_srt_list

            except (requests.exceptions.RequestException, pydantic.ValidationError, ValueError) as e:
                # 捕获所有预期的、可重试的或不可重试的错误
                log_system_event("warning", f"[字幕任务 {group_index+1}] 尝试 {attempt + 1}/{max_retries} 失败。错误: {type(e).__name__}: {e}", in_worker=True)
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = 5 * (2 ** attempt)
                    log_system_event("info", f"{wait_time}秒后将自动重试...", in_worker=True)
                    time.sleep(wait_time)
                else:
                    # 所有重试次数用尽，跳出循环
                    break
        
        # 如果循环结束（意味着所有重试都失败了），则抛出最后的异常
        log_system_event("error", f"[字幕任务 {group_index+1}] 在 {max_retries} 次尝试后仍然失败。", in_worker=True)
        raise RuntimeError(f"批次 {group_index+1} 失败") from last_exception

    except Exception as e:
        # 这是一个最终的捕获器，确保任何未预料到的错误都会被记录并重新抛出
        log_system_event("critical", f"[字幕任务 {group_index+1}] 发生未预料的严重错误: {type(e).__name__}: {e}", in_worker=True)
        # 【关键】重新抛出异常，以便 ProcessPoolExecutor 能够捕获到这个任务的失败状态
        raise e

def run_subtitle_extraction_pipeline(subtitle_config: dict, chunk_files: list[dict], update_status_callback: callable) -> str:
    """
    主调度器，负责并发处理所有音频批次并生成最终的 SRT 内容。
    (优化版 v2: 使用 ProcessPoolExecutor 实现进程隔离，避免线程死锁)
    """
    
    api_keys = subtitle_config.get('GEMINI_API_KEYS', [])
    prompt_api_url = subtitle_config.get('PROMPT_API_URL', '')
    gemini_endpoint_prefix = subtitle_config.get('GEMINI_API_ENDPOINT_PREFIX', '')
    
    if not all([api_keys, prompt_api_url, gemini_endpoint_prefix]):
        raise ValueError("字幕配置中缺少 'GEMINI_API_KEYS', 'PROMPT_API_URL', 或 'GEMINI_API_ENDPOINT_PREFIX'。")
    
    total_chunks = len(chunk_files)
    if total_chunks == 0:
        log_system_event("warning", "没有检测到有效的语音片段，无法生成字幕。", in_worker=True)
        return ""

    chunk_groups = [chunk_files[i:i + SUBTITLE_BATCH_SIZE] for i in range(0, total_chunks, SUBTITLE_BATCH_SIZE)]
    num_groups = len(chunk_groups)
    log_system_event("info", f"已将语音片段分为 {num_groups} 个批次，准备通过多进程并发处理。", in_worker=True)
    update_status_callback(stage="subtitle_transcribing", details=f"准备处理 {num_groups} 个字幕批次...")
    
    all_srt_blocks = []
    
    # 【核心修改】: 使用 ProcessPoolExecutor 替代 ThreadPoolExecutor
    # max_workers 建议不要设置得过高，因为它会消耗更多内存。4-8个进程通常是比较好的起点。
    # SUBTITLE_CONCURRENT_REQUESTS 这个全局变量的值可以根据情况调整。
    with ProcessPoolExecutor(max_workers=SUBTITLE_CONCURRENT_REQUESTS) as executor:
        # 使用字典将 future 映射回其任务索引，便于日志记录
        future_to_index = {}
        delay_between_submissions = 60.0 / SUBTITLE_REQUESTS_PER_MINUTE
        
        log_system_event("info", "开始向进程池提交所有字幕任务...", in_worker=True)
        for i, group in enumerate(chunk_groups):
            api_key_for_process = api_keys[i % len(api_keys)]
            future = executor.submit(
                _process_subtitle_batch_with_ai,
                group,
                i, # 任务索引
                api_key_for_process,
                gemini_endpoint_prefix,
                prompt_api_url
            )
            future_to_index[future] = i + 1  # 任务索引从1开始，更符合日志习惯
            
            if i < num_groups - 1:
                time.sleep(delay_between_submissions)
        
        log_system_event("info", f"所有 {num_groups} 个任务均已提交。现在开始等待并处理返回结果...", in_worker=True)
        
        completed_count = 0
        for future in as_completed(future_to_index):
            task_index = future_to_index[future]
            completed_count += 1
            
            try:
                # 【新增】为获取结果设置一个合理的超时时间（例如15分钟）
                # 这个时间应该大于单个批次处理的最大可能时间（包括重试）
                # timeout = (单个请求超时 + 重试等待) * 重试次数，再加一些余量
                # timeout = (360s + 5s*1 + 5s*2) * 3 = (375s) * 3 ~= 20分钟
                result_timeout = 10 * 60 # 20分钟
                
                # 获取已完成任务的结果。如果子进程中发生异常，.result()会重新抛出它
                result = future.result(timeout=result_timeout)
                
                if result:
                    all_srt_blocks.extend(result)
                    log_system_event("info", f"✅ 已成功处理完字幕任务 {task_index} 的结果。", in_worker=True)
                else:
                    # 这种情况理论上不应该发生，除非_process_subtitle_batch_with_ai在没有结果时返回了None或[]
                    log_system_event("warning", f"字幕任务 {task_index} 返回了空结果，可能处理失败但未抛出异常。", in_worker=True)

            except TimeoutError:
                # 【新增】捕获超时错误
                log_system_event("error", f"❌ 获取字幕任务 {task_index} 的结果超时！该子进程可能已僵死。", in_worker=True)
                # 即使一个任务超时，我们依然要继续处理其他已完成的任务
                
            except Exception as e:
                # 【关键】现在可以正确捕获子进程中的所有异常
                log_system_event("error", f"❌ 获取字幕任务 {task_index} 的结果时发生严重错误: {e}", in_worker=True)
            
            finally:
                # 无论成功失败，都更新进度
                update_status_callback(stage="subtitle_transcribing", details=f"已处理 {completed_count}/{num_groups} 个字幕批次...")

    log_system_event("info", "所有并发任务处理完成，正在整合字幕...", in_worker=True)
    
    if not all_srt_blocks:
        log_system_event("warning", "所有字幕批次处理均失败或未返回任何内容，最终生成的字幕为空。", in_worker=True)
        # 根据业务需求，可以选择返回空字符串或抛出异常
        # 这里选择返回空字符串，让主流程判断
        return ""

    all_srt_blocks.sort(key=lambda x: x["start_ms"])
    
    final_srt_lines = [f"{i + 1}\n{block['srt_line']}" for i, block in enumerate(all_srt_blocks)]
    final_srt_content = "\n".join(final_srt_lines)
    
    log_system_event("info", f"字幕整合完成，共生成 {len(all_srt_blocks)} 条字幕。", in_worker=True)
    
    return final_srt_content

# =============================================================================
# --- 第 5 步: MixFileCLI 客户端 ---
# =============================================================================

class MixFileCLIClient:
    # 一个简单的用于与 MixFileCLI 后端 API 交互的客户端。
    def __init__(self, base_url: str, proxies: dict = None):
        if not base_url.startswith("http"):
            raise ValueError("Base URL 必须以 http 或 https 开头")
        self.base_url = base_url
        self.session = requests.Session()
        # 【核心修改】设置代理
        if proxies:
            self.session.proxies = proxies

    def _make_request(self, method: str, url: str, **kwargs):
        # 统一的请求发送方法，包含错误处理。
        try:
            # 确保请求不会被缓存
            headers = kwargs.get('headers', {})
            headers.update({'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
            kwargs['headers'] = headers
            
            # 使用更长的超时时间以适应慢速网络
            response = self.session.request(method, url, timeout=600, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            error_text = str(e)
            status_code = 500 # 默认为通用服务器错误
            if e.response is not None:
                error_text = e.response.text
                status_code = e.response.status_code
            return (status_code, error_text)

    def upload_file(self, local_file_path: str, progress_callback: callable = None):
        #
        # 【修改后】上传单个文件并获取分享码，增加了进度回调功能。
        #
        # Args:
        #     local_file_path (str): 本地文件的完整路径。
        #     progress_callback (callable, optional): 进度回调函数，接收 (bytes_uploaded, total_bytes)。
        #
        # Returns:
        #     requests.Response or tuple: 成功时返回 Response 对象，失败时返回 (status_code, error_text)。
        #
        filename = os.path.basename(local_file_path)
        upload_url = urljoin(self.base_url, f"/api/upload/{quote(filename)}")
        
        try:
            file_size = os.path.getsize(local_file_path)
        except OSError as e:
             # 如果文件在这里就找不到了，直接返回错误
            return (404, f"File not found: {e}")

        # 【核心修改】创建一个生成器，它在读取文件的同时调用回调函数
        def file_reader_generator(file_handle):
            chunk_size = 1 * 1024 * 1024 # 1MB chunk
            bytes_read = 0
            while True:
                chunk = file_handle.read(chunk_size)
                if not chunk:
                    if progress_callback: # 确保最后100%被调用
                        progress_callback(file_size, file_size)
                    break
                bytes_read += len(chunk)
                if progress_callback:
                    progress_callback(bytes_read, file_size)
                yield chunk

        try:
            with open(local_file_path, 'rb') as f:
                # 将生成器作为 data 传递
                return self._make_request("PUT", upload_url, data=file_reader_generator(f))
        except FileNotFoundError as e:
            return (404, str(e))

# =============================================================================
# --- 第 6 步: 统一的 Flask API 服务 (主进程) ---
# =============================================================================

# --- A. 应用初始化与任务管理 ---
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
# 全局代理配置，将在main函数中被设置
GLOBAL_PROXY_SETTINGS = None
# 使用字典来存储所有异步任务的状态，并用锁来保证线程安全
tasks = {}
tasks_lock = threading.Lock()

# 全局变量，将在 main 函数中被初始化
api_client = None 
subtitle_config_global = {}
FRP_SERVER_ADDR = None # 声明全局变量
TASK_QUEUE = None # 多进程任务队列
RESULT_QUEUE = None # 多进程结果队列

# --- B. API 路由定义 ---

# =============================================================================
# --- (方法1/3) unified_upload_endpoint [修改后] ---
# =============================================================================
@app.route("/api/upload", methods=["POST"])
def unified_upload_endpoint():
    #
    # 统一的媒体处理入口 API。
    # 接收一个 URL，并根据参数决定是上传文件、提取字幕，还是两者都做。
    # 始终以异步模式运行。
    #
    data = request.get_json()
    if not data or "url" not in data:
        return jsonify({"error": "请求体中必须包含 'url' 字段"}), 400

    task_id = str(uuid.uuid4())
    
    # 从请求中提取参数
    request_params = {
        "url": data["url"],
        "extract_subtitle": data.get("extract_subtitle", False),
        "upload_video": data.get("upload_video", True),
        "upload_subtitle": data.get("upload_subtitle", False),
    }

    # 【核心修改】初始化新的、面板友好的任务状态结构
    with tasks_lock:
        tasks[task_id] = {
            "taskId": task_id,
            "status": "QUEUED",
            "progress": 0,
            "error": None,
            "results": {
                "video": {
                    "status": "PENDING" if request_params["upload_video"] else "SKIPPED",
                    "details": "等待处理" if request_params["upload_video"] else "用户未请求此操作",
                    "output": None,
                    "error": None
                },
                "subtitle": {
                    "status": "PENDING" if request_params["extract_subtitle"] else "SKIPPED",
                    "details": "等待处理" if request_params["extract_subtitle"] else "用户未请求此操作",
                    "output": None,
                    "error": None
                }
            },
            "createdAt": time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime()),
            "updatedAt": time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
        }

    # 将任务数据放入队列，由子进程处理
    task_data = {
        'task_id': task_id,
        'params': request_params,
        'subtitle_config': subtitle_config_global,
        'api_client_base_url': api_client.base_url,
        'frp_server_addr': FRP_SERVER_ADDR
    }
    TASK_QUEUE.put(task_data)
    
    log_system_event("info", f"已创建新任务 {task_id} 并推入处理队列。")

    # 返回体保持不变，依然简洁
    return jsonify({
        "task_id": task_id,
        "status_url": f"/api/tasks/{task_id}"
    }), 202

@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task_status_endpoint(task_id):
    # 获取指定任务的当前状态和结果。
    with tasks_lock:
        task = tasks.get(task_id)
    
    if task:
        return jsonify(task)
    else:
        return jsonify({"error": "未找到指定的 task_id"}), 404


@app.route('/killer_status_frp', methods=['GET'])
def health_status_endpoint():
    # 用于外部健康检查的简单端点。
    return jsonify({
        "status": "killer_api_is_running_via_frp",
        "message": "统一媒体处理 API 服务正在运行，并通过 FRP 暴露。",
        "timestamp": time.time()
    }), 200


@app.route('/force_shutdown_notebook', methods=['GET'])
def force_shutdown_endpoint():
    #
    # 远程关闭 API，接收一个 token 进行验证。
    #
    log_system_event("info", "API /force_shutdown_notebook 被调用...")
    token_from_request = request.args.get('token')

    if token_from_request != KILLER_API_SHUTDOWN_TOKEN:
        log_system_event("error", "API Auth 失败: Token 无效。")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    
    log_system_event("info", "API Auth 成功。正在安排后台关闭任务...")

    def delayed_full_shutdown():
        # Step 1: 先平滑地杀死子进程
        log_system_event("info", "后台关闭任务：开始终止子进程...")
        _find_and_kill_targeted_processes(signal.SIGTERM)
        time.sleep(2)
        
        # Step 2: 强制终止仍然存在的子进程
        log_system_event("info", "后台关闭任务：强制终止任何残留子进程...")
        _find_and_kill_targeted_processes(signal.SIGKILL)
        time.sleep(1)

        # Step 3: 终止主内核
        _shutdown_notebook_kernel_immediately()
    
    # 立即启动后台线程，然后立刻返回响应，确保调用方收到确认
    threading.Thread(target=delayed_full_shutdown, daemon=True).start()
    
    return jsonify({
        "status": "shutdown_initiated",
        "message": "关闭信号已接收。后台正在执行清理和内核关闭操作。",
    }), 200

# =============================================================================
# --- 第 7 步: 高容错的统一任务处理器 (在子进程中运行) ---
# =============================================================================

def process_unified_task(task_data: dict, result_queue: multiprocessing.Queue, upload_queue: multiprocessing.Queue, subtitle_config: dict, ai_models: dict):
    """
    【最终修复版】修复了状态报告延迟问题，确保API状态与后台操作严格同步。
    """
    task_id = task_data['task_id']
    params = task_data['params']
    temp_dir = Path(f"/kaggle/working/task_{task_id}")
    temp_dir.mkdir(exist_ok=True)
    
    # ... [内部状态 _internal_status 和 _update_status 函数保持不变] ...
    _internal_status = {
        "progress": 0,
        "results": {
            "video": {"status": "PENDING", "details": "准备中", "output": None, "error": None},
            "subtitle": {"status": "PENDING", "details": "准备中", "output": None, "error": None}
        }
    }

    def _update_status(component=None, status=None, details=None, progress_val=None, output=None, error_obj=None):
        payload = {'type': 'status_update', 'task_id': task_id, 'payload': {}}
        update_target = {}
        if component and component in _internal_status["results"]:
            update_target = _internal_status["results"][component]
            partial_update = {}
            if status: update_target["status"] = status; partial_update['status'] = status
            if details: update_target["details"] = details; partial_update['details'] = details
            if output:
                if update_target.get("output") is None: update_target["output"] = {}
                update_target["output"].update(output)
                partial_update['output'] = update_target['output']
            if error_obj: update_target["error"] = error_obj; partial_update['error'] = error_obj
            payload['payload']['results'] = {component: partial_update}
        if progress_val is not None:
            _internal_status["progress"] = progress_val
            payload['payload']['progress'] = _internal_status['progress']
        if payload['payload']:
             result_queue.put(payload)
             
    try:
        # --- 步骤 1: 下载文件 ---
        _update_status(component="video", status="RUNNING", details="开始下载文件...", progress_val=5)
        _update_status(component="subtitle", status="RUNNING", details="等待视频下载...")
        file_url = params['url']
        filename = unquote(file_url.split("/")[-1].split("?")[0] or f"file_{task_id}")
        local_file_path = temp_dir / filename

        with requests.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            bytes_downloaded = 0
            with open(local_file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    if total_size > 0:
                        dl_progress = round((bytes_downloaded / total_size) * 100)
                        _update_status(component="video", details=f"下载中 ({dl_progress}%)...", progress_val=5 + int(dl_progress * 0.2))
        
        # 【核心修改点 1】下载完成后，明确更新video状态
        _update_status(component="video", details="下载完成，准备音频提取...")


        # --- 步骤 2: (如果需要) 串行执行有冲突的音频预处理 ---
        audio_chunks = None
        if params["extract_subtitle"]:
            mime_type, _ = mimetypes.guess_type(local_file_path)
            if not (mime_type and mime_type.startswith("video")):
                _update_status(component="subtitle", status="SKIPPED", details="源文件不是视频格式")
            else:
                try:
                    _update_status(component="subtitle", status="RUNNING", details="正在预处理音频...", progress_val=30)
                    def sub_progress_callback(stage, details): _update_status(component="subtitle", details=details)
                    # 这个函数包含了ffmpeg调用和后续的VAD等步骤，它是阻塞的
                    audio_chunks = preprocess_audio_for_subtitles(local_file_path, temp_dir, sub_progress_callback, ai_models)
                except Exception as e:
                    log_system_event("error", f"音频预处理失败: {e}", in_worker=True)
                    _update_status(component="subtitle", status="FAILED", details=f"音频预处理失败: {e}", error_obj={"code": "AUDIO_EXTRACTION_FAILED", "message": str(e)})
                    audio_chunks = None
        else:
            _update_status(component="subtitle", status="SKIPPED", details="用户未请求提取")

        # --- 步骤 3: 并行执行视频上传和字幕AI处理 ---
        # 此时音频预处理已结束，视频文件已释放，可以安全地开始上传
        if params["upload_video"]:
            # 【核心修改点 2】在派发任务前，立即更新状态
            _update_status(component="video", status="RUNNING", details="已派发上传任务 (0%)...", progress_val=35)
            upload_queue.put({
                'task_id': task_id, 'component': 'video', 'local_file_path': str(local_file_path),
                'filename_for_link': filename, 'api_client_base_url': task_data['api_client_base_url'],
                'frp_server_addr': task_data['frp_server_addr']
            })
        else:
            _update_status(component="video", status="SKIPPED", details="用户未请求上传")
        
        # 同时，如果音频块已成功提取，则开始进行耗时的AI处理
        if audio_chunks is not None:
            try:
                _update_status(component="subtitle", status="RUNNING", details="AI处理中...", progress_val=40)
                def sub_progress_callback(stage, details): _update_status(component="subtitle", details=details)
                srt_content = run_subtitle_extraction_pipeline(subtitle_config, audio_chunks, sub_progress_callback)
                
                if not srt_content: raise RuntimeError("未能生成有效的字幕内容。")
                
                srt_base64 = base64.b64encode(srt_content.encode('utf-8')).decode('utf-8')
                _update_status(component="subtitle", output={"contentBase64": srt_base64})
                
                if params["upload_subtitle"]:
                    _update_status(component="subtitle", status="RUNNING", details="已派发字幕上传任务")
                    srt_filename = local_file_path.stem + ".srt"
                    srt_path = temp_dir / srt_filename
                    with open(srt_path, "w", encoding="utf-8") as f: f.write(srt_content)
                    upload_queue.put({
                       'task_id': task_id, 'component': 'subtitle', 'local_file_path': str(srt_path),
                       'filename_for_link': srt_filename, 'api_client_base_url': task_data['api_client_base_url'],
                       'frp_server_addr': task_data['frp_server_addr']
                    })
                else:
                    _update_status(component="subtitle", status="SUCCESS", details="提取成功")
            except Exception as e:
                _update_status(component="subtitle", status="FAILED", details=str(e), error_obj={"code": "TRANSCRIPTION_FAILED", "message": str(e)})

        log_system_event("info", f"媒体处理进程为任务 {task_id} 的主要工作已完成。", in_worker=True)

    except Exception as e:
        log_system_event("error", f"处理任务 {task_id} 时发生严重错误: {e}", in_worker=True)
        result_queue.put({'type': 'task_result', 'task_id': task_id, 'status': 'FAILED', 'result': {}, 'error': {"code": "FATAL_ERROR", "message": str(e)}})
    finally:
        log_system_event("info", f"媒体处理进程 {task_id} 完成，不再清理主任务目录。", in_worker=True)

# =============================================================================
# --- 第 8 步: 多进程 Worker 与主程序 ---
# =============================================================================


# =============================================================================
# --- (新增方法 1/3) uploader_process_loop [全新] ---
# =============================================================================

# 这个新函数专门负责文件上传，运行在独立的进程中
def uploader_process_loop(upload_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue):
    """
    【修改后】一个专用的上传工作进程，增加了详细的进度报告功能，以解决静默挂起问题。
    """
    log_system_event("info", "上传专用工作进程已启动。", in_worker=True)
    
    while True:
        try:
            upload_task_data = upload_queue.get()
            if upload_task_data is None:
                break

            task_id = upload_task_data['task_id']
            component = upload_task_data['component']
            local_file_path_str = upload_task_data['local_file_path']

            # --- 定义一个状态更新的快捷方式 ---
            def _update_uploader_status(status, details=None, output=None, error_obj=None):
                payload_results = {component: {}}
                if status: payload_results[component]['status'] = status
                if details: payload_results[component]['details'] = details
                if output: payload_results[component]['output'] = output
                if error_obj: payload_results[component]['error'] = error_obj
                result_queue.put({'type': 'status_update', 'task_id': task_id, 'payload': {'results': payload_results}})

            try:
                filename_for_link = upload_task_data['filename_for_link']
                api_client_base_url = upload_task_data['api_client_base_url']
                frp_server_addr = upload_task_data['frp_server_addr']
                
                # api_client = MixFileCLIClient(base_url=api_client_base_url)
                api_client = MixFileCLIClient(base_url=api_client_base_url, proxies=GLOBAL_PROXY_SETTINGS)
                log_system_event("info", f"[上传进程] [{component}] 开始处理上传任务，文件: {local_file_path_str}", in_worker=True)
                
                _update_uploader_status("RUNNING", details="正在上传 (0%)...")

                # 【核心修改】定义并使用进度回调
                # 使用 nonlocal 关键字来修改外部作用域的变量
                last_reported_percent = -1
                def progress_callback(bytes_uploaded, total_bytes):
                    nonlocal last_reported_percent
                    if total_bytes > 0:
                        percent = int((bytes_uploaded / total_bytes) * 100)
                        # 每 5% 更新一次状态，避免消息过多，同时确保 100% 会被报告
                        if percent > last_reported_percent and (percent % 5 == 0 or percent == 100):
                            _update_uploader_status("RUNNING", details=f"正在上传 ({percent}%)")
                            last_reported_percent = percent
                
                log_system_event("info", f"[上传进程] [{component}] 准备调用 api_client.upload_file (带进度回调)", in_worker=True)
                # 将回调函数传递给 upload_file 方法
                response = api_client.upload_file(local_file_path_str, progress_callback=progress_callback)
                log_system_event("info", f"[上传进程] [{component}] api_client.upload_file 调用已返回", in_worker=True)
                
                if isinstance(response, requests.Response):
                    if response.ok:
                        share_code = response.text.strip()
                        share_url = f"http://{frp_server_addr}:{MIXFILE_REMOTE_PORT}/api/download/{quote(filename_for_link)}?s={share_code}"
                        _update_uploader_status("SUCCESS", details="上传成功", output={"shareUrl": share_url})
                        log_system_event("info", f"✅ [上传进程] [{component}] 上传成功。", in_worker=True)
                    else:
                        error_msg = f"HTTP {response.status_code}: {response.text}"
                        raise RuntimeError(error_msg)
                elif isinstance(response, tuple):
                    error_msg = f"请求失败 (状态码 {response[0]}): {response[1]}"
                    raise RuntimeError(error_msg)
                else:
                    error_msg = f"MixFile客户端返回了未知类型: {type(response)}"
                    raise RuntimeError(error_msg)

            except Exception as e:
                log_system_event("error", f"❌ [上传进程] [{component}] 上传任务发生严重错误: {e}", in_worker=True)
                _update_uploader_status("FAILED", details=f"上传失败: {e}", error_obj={"code": "UPLOAD_FAILED", "message": str(e)})

            finally:
                # 文件清理逻辑保持不变
                try:
                    local_file_path_obj = Path(local_file_path_str)
                    if local_file_path_obj.exists():
                        local_file_path_obj.unlink()
                        log_system_event("info", f"[上传进程] [{component}] 已清理文件: {local_file_path_str}", in_worker=True)
                except OSError as e:
                    log_system_event("warning", f"[上传进程] [{component}] 清理文件 {local_file_path_str} 失败: {e}", in_worker=True)

        except KeyboardInterrupt:
            break
        except Exception as e:
            log_system_event("critical", f"上传专用工作进程发生致命错误，循环可能终止: {e}", in_worker=True)
            
    log_system_event("info", "上传专用工作进程已关闭。", in_worker=True)

def worker_process_loop(task_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue, upload_queue: multiprocessing.Queue):
    """
    媒体处理工作进程循环，负责媒体处理并将上传任务外包。
    """
    log_system_event("info", "媒体处理工作进程已启动。", in_worker=True)
    ai_models = load_ai_models()
    # subtitle_config_global 是在main函数中解密的全局变量，子进程可以直接访问
    
    while True:
        try:
            task_data = task_queue.get()
            if task_data is None:
                break
            log_system_event("info", f"媒体处理进程接收到新任务: {task_data['task_id']}", in_worker=True)
            process_unified_task(task_data, result_queue, upload_queue, subtitle_config_global, ai_models)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log_system_event("critical", f"媒体处理工作进程循环发生严重错误: {e}", in_worker=True)
    log_system_event("info", "媒体处理工作进程已关闭。", in_worker=True)

def result_processor_thread_loop(result_queue: multiprocessing.Queue):
    """
    【修改后】结果处理器，增加了在任务达到最终状态后，负责清理任务临时目录的逻辑。
    """
    log_system_event("info", "结果处理线程已启动。")
    while True:
        try:
            # 使用长超时以防队列长时间空闲
            result_data = result_queue.get(timeout=3600)
            task_id = result_data['task_id']

            with tasks_lock:
                if task_id not in tasks:
                    continue

                task = tasks[task_id]
                task['updatedAt'] = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
                
                data_type = result_data.get('type')
                
                # --- 状态更新逻辑 ---
                if data_type == 'status_update':
                    payload = result_data.get('payload', {})
                    if 'progress' in payload: task['progress'] = payload['progress']
                    if 'results' in payload:
                        for component, data in payload['results'].items():
                            if component in task['results']:
                                task['results'][component].update(data)
                    # 只要有更新，就认为是RUNNING（除非已经是最终状态）
                    if task['status'] not in ["SUCCESS", "FAILED", "PARTIAL_SUCCESS"]:
                         task['status'] = "RUNNING"

                # --- 任务级结果（通常是严重错误） ---
                elif data_type == 'task_result':
                    task['status'] = result_data.get('status', 'FAILED')
                    task['error'] = result_data.get('error')
                    task['progress'] = 100
                
                # --- 检查任务是否已达到最终状态 ---
                video_status = task['results']['video']['status']
                subtitle_status = task['results']['subtitle']['status']
                
                # 检查所有子组件是否都已脱离“处理中”的状态
                is_finished = "PENDING" not in (video_status, subtitle_status) and \
                              "RUNNING" not in (video_status, subtitle_status)

                # 如果任务已完成，并且尚未设置最终状态，则进行评估和清理
                if is_finished and task['status'] not in ["SUCCESS", "FAILED", "PARTIAL_SUCCESS"]:
                    log_system_event("info", f"任务 {task_id} 所有组件已完成，正在进行最终状态评估。")
                    task['progress'] = 100
                    
                    # 状态清理：处理因进程崩溃导致的 "RUNNING" 残留状态
                    for component in task['results']:
                        comp_data = task['results'][component]
                        if comp_data['status'] == 'RUNNING':
                            comp_data['status'] = 'FAILED'
                            comp_data['details'] = '组件因未知原因未能完成'
                            comp_data['error'] = { 'code': 'WORKER_CRASHED', 'message': '处理此组件的工作进程可能已意外终止。'}

                    # 重新获取清理后的状态
                    final_video_status = task['results']['video']['status']
                    final_subtitle_status = task['results']['subtitle']['status']
                    
                    # 定义参与最终状态评估的状态列表 (排除 SKIPPED)
                    active_statuses = [s for s in (final_video_status, final_subtitle_status) if s != "SKIPPED"]
                    
                    if not active_statuses: # 如果所有组件都被跳过
                        task['status'] = "SUCCESS"
                    elif all(s == "SUCCESS" for s in active_statuses):
                        task['status'] = "SUCCESS"
                    elif "FAILED" in active_statuses:
                        if "SUCCESS" in active_statuses:
                            task['status'] = "PARTIAL_SUCCESS"
                        else:
                            task['status'] = "FAILED"
                    else: # 其他情况，例如全是SKIPPED和SUCCESS
                        task['status'] = "SUCCESS"

                    log_system_event("info", f"任务 {task_id} 最终状态被设置为: {task['status']}")

                    # 【核心新增】在此处执行清理操作
                    temp_dir_to_clean = Path(f"/kaggle/working/task_{task_id}")
                    if temp_dir_to_clean.exists():
                        log_system_event("info", f"任务 {task_id} 已结束，准备清理临时目录: {temp_dir_to_clean}")
                        try:
                            shutil.rmtree(temp_dir_to_clean)
                            log_system_event("info", f"✅ 成功清理任务 {task_id} 的临时目录。")
                        except Exception as e:
                            log_system_event("error", f"❌ 清理任务 {task_id} 的临时目录失败: {e}")

        except QueueEmpty:
            # 队列为空是正常情况，继续循环
            continue
        except Exception as e:
            log_system_event("error", f"结果处理线程发生严重错误: {e}")

def main():
    #
    # 主执行函数，负责初始化和启动所有服务。
    #
    global api_client, subtitle_config_global, FRP_SERVER_ADDR
    global TASK_QUEUE, RESULT_QUEUE, UPLOAD_QUEUE # <--- 新增 UPLOAD_QUEUE

    try:
        # --- 1. 启动前准备 ---
        log_system_event("info", "服务正在启动...")
        
        install_torch_cmd = (
            "pip uninstall -y torch torchvision torchaudio && "
            "pip install torch==2.3.0 torchaudio==2.3.0 --index-url https://download.pytorch.org/whl/cu121"
        )
        log_system_event("info", "正在安装兼容的 PyTorch 版本...")
        install_proc = subprocess.run(install_torch_cmd, shell=True, capture_output=True, text=True)
        if install_proc.returncode != 0:
            log_system_event("error", f"PyTorch 安装失败！\nStdout: {install_proc.stdout}\nStderr: {install_proc.stderr}")
            raise RuntimeError("未能安装兼容的 PyTorch 版本。")
        log_system_event("info", "✅ 兼容的 PyTorch 安装完成。")
        
        install_other_cmd = "pip install -q pydantic pydub faster-whisper@https://github.com/SYSTRAN/faster-whisper/archive/refs/heads/master.tar.gz denoiser google-generativeai requests psutil"
        log_system_event("info", "正在安装其余依赖库...")
        subprocess.run(install_other_cmd, shell=True, check=True)
        log_system_event("info", "✅ 其余依赖库安装完成。")
        
        check_environment()
        
        # --- 设置多进程启动方法 ---
        multiprocessing.set_start_method('fork', force=True)

        # --- 2. 解密配置 ---
        frp_config = get_decrypted_config(ENCRYPTED_FRP_CONFIG, "FRP")
        subtitle_config_global = get_decrypted_config(ENCRYPTED_SUBTITLE_CONFIG, "Subtitle")
        FRP_SERVER_ADDR = frp_config['FRP_SERVER_ADDR']
        FRP_SERVER_PORT = frp_config['FRP_SERVER_PORT']
        FRP_TOKEN = frp_config['FRP_TOKEN']
        
        # --- 3. 初始化 MixFile 客户端 ---
        api_client_base_url = f"http://127.0.0.1:{MIXFILE_LOCAL_PORT}"
        api_client = MixFileCLIClient(base_url=api_client_base_url)

        # --- 4. 启动 MixFileCLI 服务 ---
        log_system_event("info", "正在创建 MixFileCLI config.yml...")
        with open("config.yml", "w") as f: f.write(mixfile_config_yaml)
        log_system_event("info", "正在下载并启动 MixFileCLI...")
        if not os.path.exists("mixfile-cli.jar"):
            run_command("wget -q --show-progress https://raw.githubusercontent.com/jornhand/kagglewithmixfile/refs/heads/main/mixfile-cli-2.0.1.jar -O mixfile-cli.jar").wait()
        run_command("java -jar mixfile-cli.jar", "mixfile.log")
        if not wait_for_port(MIXFILE_LOCAL_PORT):
            raise RuntimeError("MixFileCLI 服务启动失败，请检查 mixfile.log。")

        # 【核心新增】--- 4.5 启动并配置最优代理 ---
        v2ray_sub_url = subtitle_config_global.get("V2RAY_SUB_URL")
        if v2ray_sub_url:
            proxy_manager = ProxyManager(sub_url=v2ray_sub_url, mixfile_base_url=api_client_base_url)
            proxy_manager.setup_best_proxy()
        else:
            log_system_event("info", "未在配置中找到 V2RAY_SUB_URL，不启用代理功能。")

        # --- 5. 初始化多进程队列和工作进程 ---
        TASK_QUEUE = multiprocessing.Queue()
        RESULT_QUEUE = multiprocessing.Queue()
        UPLOAD_QUEUE = multiprocessing.Queue() # 新增上传队列

        # 启动媒体处理工作进程
        media_worker = multiprocessing.Process(
            target=worker_process_loop,
            args=(TASK_QUEUE, RESULT_QUEUE, UPLOAD_QUEUE), 
            daemon=False
        )
        media_worker.start()
        
        # 启动专用的上传工作进程
        uploader_worker = multiprocessing.Process(
            target=uploader_process_loop,
            args=(UPLOAD_QUEUE, RESULT_QUEUE),
            daemon=False
        )
        uploader_worker.start()

        # 启动结果处理线程
        result_thread = threading.Thread(
            target=result_processor_thread_loop,
            args=(RESULT_QUEUE,),
            daemon=True
        )
        result_thread.start()

        # --- 6. 启动 Flask API 服务 ---
        def run_flask_app():
            app.run(host='0.0.0.0', port=FLASK_API_LOCAL_PORT, debug=False, use_reloader=False)
        log_system_event("info", "正在后台启动 Flask API 服务...")
        threading.Thread(target=run_flask_app, daemon=True).start()
        if not wait_for_port(FLASK_API_LOCAL_PORT):
             raise RuntimeError("Flask 服务启动失败。")

        # --- 7. 启动 frpc 客户端 ---
        log_system_event("info", "正在准备 frpc 客户端...")
        if not os.path.exists("/kaggle/working/frpc"):
            run_command("wget -q https://github.com/fatedier/frp/releases/download/v0.54.0/frp_0.54.0_linux_amd64.tar.gz && tar -zxvf frp_0.54.0_linux_amd64.tar.gz && mv frp_0.54.0_linux_amd64/frpc /kaggle/working/frpc && chmod +x /kaggle/working/frpc").wait()
        
        frpc_ini = f"""
[common]
server_addr = {FRP_SERVER_ADDR}
server_port = {FRP_SERVER_PORT}
token = {FRP_TOKEN}
log_file = /kaggle/working/frpc.log
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
        with open('frpc.ini', 'w') as f: f.write(frpc_ini)
        run_command('/kaggle/working/frpc -c ./frpc.ini')
        log_system_event("info", "frpc 客户端已在后台启动。")
        time.sleep(3)
        
        # --- 8. 最终状态报告与保活 ---
        public_api_base_url = f"http://{FRP_SERVER_ADDR}:{FLASK_API_REMOTE_PORT}"
        print("\n" + "="*60)
        print("🎉 所有服务均已成功启动！您的统一 API 已上线。")
        print(f"  -> API 入口 (POST) : {public_api_base_url}/api/upload")
        print(f"  -> 任务状态 (GET)  : {public_api_base_url}/api/tasks/<task_id>")
        print(f"  -> 健康检查 (GET)  : {public_api_base_url}/killer_status_frp")
        print(f"  -> 远程关闭 (GET)  : {public_api_base_url}/force_shutdown_notebook?token={KILLER_API_SHUTDOWN_TOKEN}")
        print("="*60)
        
        while True:
            time.sleep(300)
            all_workers_alive = media_worker.is_alive() and uploader_worker.is_alive()
            worker_status_msg = '全部存活' if all_workers_alive else '存在已退出的工作进程'
            log_system_event("info", f"服务持续运行中... ({time.ctime()}) 工作进程状态: {worker_status_msg}")
            
    except (DecryptionError, RuntimeError, ValueError) as e:
        log_system_event("critical", f"程序启动过程中发生致命错误: {e}")
        log_system_event("critical", "服务无法启动，程序将终止。")
    except KeyboardInterrupt:
        log_system_event("info", "服务已手动停止。")
        if 'TASK_QUEUE' in globals() and TASK_QUEUE is not None:
            TASK_QUEUE.put(None)
        if 'UPLOAD_QUEUE' in globals() and UPLOAD_QUEUE is not None:
            UPLOAD_QUEUE.put(None)
    except Exception as e:
        log_system_event("critical", f"发生未知的致命错误: {e}")

if __name__ == '__main__':
    main()