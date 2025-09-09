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
from datetime import datetime, timedelta
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
upload_task: 20
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
ENCRYPTED_FRP_CONFIG = "gAAAAABovr0nLhcvqidm1uzqWvVToI9StZZAAa9vgtqoxlXSKuMosiX8yEzEIwPrDghud3lGK0zRB0pLsc14E8Bvbk8OxSQVL_NT31PHrXsz6ulKWJYo6fNU3ycOuBoeSWLkou4-HP33INu5SyWH5rnLqvGW8KuFcZtV9qtM-A3zX-dDzDMdhBo5NeS_u5yHpCKy478r029Y"

# 第二个字符串用于字幕服务 (Gemini API 密钥等)
ENCRYPTED_SUBTITLE_CONFIG = "gAAAAABovr1QY48Fe_jcPlBAYeYyO8Dx2woxKVlQcukRmw-Uh0mVw63YT0OBVdmXMaF5Ksj-zTZSdjAvJtpht7sjx7WtjMSXeYzB_dzqrb43X7zqGN_T-blYp285D33QUnr0tfCVR4j9BuTP7KquQNm7yrFhV0zOXOnherA2OoP9htPWJ3Z45igpAj_LxYh4cOXRtPMCGdRza5vnBjlgWyuQVTg6p-1kYfiPi9lGzwgGoqoSsfJkK69TFWUM9IRrrIIz56urbQOsvYP7wtM1pqJ0ReKKRSWq6A4mXVJKMJj0Su1tuQTPSX8otiM3Y2EYwFy6J7JXdVs6zbTSRFzzQ05JXSpEl0eZ62G91ZnBtmpOArrUvqh0JsWfWVUk3D2-ijxfBnVcJ3xu1y6slW2jPtS7J2PBI_lgi939-aTecrKYl-yPj7XbxeAvSZecl_tJj8l0p1XMDmFWNol_A4UeRxI8exMCQd9QCXyTT07kIo2CF5r32FF0yPIlCdKmw-0petX2wnYxnkrG" # 示例，请替换

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
    【重构版】按需为上传任务寻找最优代理线路的工具类。
    它被设计为在工作进程中按需实例化和使用，不管理常驻后台进程。
    """
    def __init__(self, sub_url=None):
        self.sub_url = sub_url
        self.xray_path = Path("/kaggle/working/xray")
        self.config_path = Path("/kaggle/working/xray_config.json")
        self.local_socks_port = 10808
        self.geoip_path = Path("/kaggle/working/geoip.dat")
        self.geosite_path = Path("/kaggle/working/geosite.dat")

    def _ensure_xray_assets(self):
        """确保 Xray 核心及数据库文件已下载。"""
        if self.xray_path.exists() and self.geoip_path.exists() and self.geosite_path.exists():
            return

        log_system_event("info", "检测到Xray组件缺失，正在下载...", in_worker=True)
        xray_url = "https://github.com/XTLS/Xray-core/releases/download/v1.8.10/Xray-linux-64.zip"
        geoip_url = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"
        geosite_url = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat"
        zip_path = Path("/kaggle/working/xray.zip")
        
        try:
            if not self.xray_path.exists():
                log_system_event("info", "  -> 下载 Xray core...", in_worker=True)
                run_command(f"wget -q -O {zip_path} {xray_url}").wait()
                run_command(f"unzip -o {zip_path} xray -d /kaggle/working/").wait()
                self.xray_path.chmod(0o755)
                zip_path.unlink()
            if not self.geoip_path.exists():
                log_system_event("info", "  -> 下载 geoip.dat...", in_worker=True)
                run_command(f"wget -q -O {self.geoip_path} {geoip_url}").wait()
            if not self.geosite_path.exists():
                log_system_event("info", "  -> 下载 geosite.dat...", in_worker=True)
                run_command(f"wget -q -O {self.geosite_path} {geosite_url}").wait()
            log_system_event("info", "✅ Xray组件下载完成。", in_worker=True)
        except Exception as e:
            raise RuntimeError(f"下载 Xray 组件失败: {e}")

    def _fetch_and_parse_subscription(self):
        """获取并解析订阅链接，返回节点链接列表。"""
        if not self.sub_url:
            return []
        log_system_event("info", f"正在从 {self.sub_url[:30]}... 获取订阅...", in_worker=True)
        try:
            response = requests.get(self.sub_url, timeout=20)
            response.raise_for_status()
            decoded_content = base64.b64decode(response.content).decode('utf-8')
            return decoded_content.strip().split('\n')
        except Exception as e:
            log_system_event("error", f"获取或解析订阅失败: {e}", in_worker=True)
            return []

    def _generate_node_config(self, node_url):
        """根据节点URL生成Xray的JSON配置（包含路由）。"""
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
                        { "type": "field", "ip": ["geoip:private"], "outboundTag": "direct" },
                        { "type": "field", "domain": ["geosite:category-ads-all"], "outboundTag": "block" },
                    ]
                }
            }
            return config, node_name
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, f"Error parsing node '{node_name}': {e}"

    def _test_upload_speed(self, test_upload_url, proxies=None):
        """
        【状态隔离修复版】测试上传速度的核心函数。
        它接收一个完整的、包含唯一文件名的 URL 进行测试。
        """
        try:
            # 使用较小的测试数据以加快测速过程
            test_data_size = 5 * 1024 * 1024  # 1MB
            test_data = os.urandom(test_data_size)
            
            start_time = time.time()
            # 直接使用传入的、唯一的 URL
            response = requests.put(test_upload_url, data=test_data, proxies=proxies, timeout=30)
            end_time = time.time()
            
            response.raise_for_status() # 确保上传成功 (返回 2xx 状态码)

            # 注意：我们不再尝试删除测速文件，因为它们的文件名是唯一的，
            # 留存在服务器上不会对后续操作产生冲突。
            # MixFileCLI 服务重启或手动清理即可。

            duration = end_time - start_time
            if duration > 0:
                return (test_data_size / duration) / (1024 * 1024)  # MB/s
            else:
                # 如果时间过短，给一个极高的速度值，而不是0，以表示连接非常快
                return 999 
        except Exception as e:
            log_system_event("debug", f"测速失败 (proxy: {bool(proxies)}): {type(e).__name__}", in_worker=True)
            return 0

    def get_best_proxy_for_upload(self, api_client_base_url):
        """
        【状态隔离最终版】执行完整的按需测速流程。
        为每一次独立的测速（包括直连）都生成一个唯一的上传URL，确保无状态冲突。
        """
        log_system_event("info", "====== 开始按需测速 ======", in_worker=True)
        self._ensure_xray_assets()

        # --- 内部辅助函数，用于生成唯一的测试URL ---
        def generate_unique_test_url():
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            test_filename = f"speed_test_{random_suffix}.tmp"
            return urljoin(api_client_base_url, f"/api/upload/{quote(test_filename)}")
        print("正在测试下载速度...")
        
        download_speed = wifi.download() / 1_000_000  # 转换为Mbps
        print(f"下载速度: {download_speed:.2f} Mbps")

        print("正在测试上传速度...")
        upload_speed = wifi.upload() / 1_000_000  # 转换为Mbps
        print(f"上传速度: {upload_speed:.2f} Mbps")     
        # 1. 测试直连速度
        direct_test_url = generate_unique_test_url()
        direct_speed = self._test_upload_speed(direct_test_url)
  
        log_system_event("info", f"  -> 直连速度: {direct_speed:.2f} MB/s", in_worker=True)

        best_node_config = None
        best_node_speed = direct_speed
        best_node_name = "Direct Connection"

        # 2. 循环测试所有代理节点
        node_urls = self._fetch_and_parse_subscription()
        if not node_urls:
            log_system_event("warning", "未获取到代理节点，将使用直连。", in_worker=True)
            log_system_event("info", "====== 测速结束 ======", in_worker=True)
            return None, None

        # 限制测速节点数量
        num_to_test = 3 if len(node_urls) > 3 else len(node_urls)
        nodes_to_test = random.sample(node_urls, num_to_test)
        log_system_event("info", f"随机选择 {len(nodes_to_test)} 个节点进行测速...", in_worker=True)

        for node_url in nodes_to_test:
            node_config, node_name = self._generate_node_config(node_url.strip())
            if not node_config: 
                log_system_event("warning", f"解析节点失败，跳过: {node_name}", in_worker=True)
                continue

            log_system_event("info", f"  -> 正在测试节点: {node_name}...", in_worker=True)
            with open(self.config_path, 'w') as f:
                json.dump(node_config, f)
            
            process = run_command(f"{self.xray_path} -c {self.config_path}")
            if not wait_for_port(self.local_socks_port, host='127.0.0.1', timeout=10):
                log_system_event("warning", f"     节点 {node_name} 启动失败。", in_worker=True)
                process.terminate()
                process.wait()
                continue
            
            proxies = {'http': f'socks5h://127.0.0.1:{self.local_socks_port}', 'https': f'socks5h://127.0.0.1:{self.local_socks_port}'}
            
            # 【核心修改】为每个代理节点也生成唯一的测试URL
            node_test_url = generate_unique_test_url()
            node_speed = self._test_upload_speed(node_test_url, proxies=proxies)
            log_system_event("info", f"     节点 {node_name} 速度: {node_speed:.2f} MB/s", in_worker=True)

            if node_speed > best_node_speed:
                best_node_speed = node_speed
                best_node_config = node_config
                best_node_name = node_name

            process.terminate()
            process.wait()
            time.sleep(1)

        log_system_event("info", "="*28, in_worker=True)
        log_system_event("info", f"  最优线路: {best_node_name}", in_worker=True)
        log_system_event("info", f"  最高速度: {best_node_speed:.2f} MB/s", in_worker=True)
        log_system_event("info", "="*28, in_worker=True)
        log_system_event("info", "====== 测速结束 ======", in_worker=True)

        if best_node_config:
            return {'http': f'socks5h://127.0.0.1:{self.local_socks_port}', 'https': f'socks5h://127.0.0.1:{self.local_socks_port}'}, best_node_config
        else:
            return None, None
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

def extract_audio_with_ffmpeg(
    video_path: Path,
    temp_dir: Path,
    update_status_callback: callable
) -> Path:
    """
    【新增】只负责从视频中提取音频的函数。
    这个过程会锁定视频文件，完成后即释放。
    返回提取出的音频文件路径。
    """
    update_status_callback(stage="subtitle_extract_audio", details="正在从视频中提取音频 (ffmpeg)...")
    raw_audio_path = temp_dir / "raw_audio.wav"
    try:
        command = [
            "ffmpeg", "-i", str(video_path),
            "-ac", "1", "-ar", "16000",
            "-vn", "-y", "-loglevel", "error", str(raw_audio_path)
        ]
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        return raw_audio_path
    except subprocess.CalledProcessError as e:
        log_system_event("error", f"FFmpeg 提取音频失败。Stderr: {e.stderr}", in_worker=True)
        raise RuntimeError(f"FFmpeg 提取音频失败: {e.stderr}")

def preprocess_audio_for_subtitles(
    raw_audio_path: Path, # <--- 输入参数变更
    temp_dir: Path,
    update_status_callback: callable,
    ai_models: dict
) -> list[dict]:
    """
    【修改后】接收一个已提取的音频文件，执行后续的降噪、VAD切分。
    这个过程不再接触原始视频文件。
    """
    # 1. 检查降噪模型
    denoiser_model = ai_models.get("denoiser")
    if denoiser_model:
        update_status_callback(stage="subtitle_denoise", details="AI 降噪模型已加载，准备处理...")
        log_system_event("info", "将使用预加载的 AI 降噪模型。", in_worker=True)
    else:
        log_system_event("warning", "降噪模型不可用，将跳过降噪步骤。", in_worker=True)

    # 2. 分块处理音频：降噪 -> VAD
    update_status_callback(stage="subtitle_vad", details="正在进行音频分块与语音检测...")
    try:
        original_audio = AudioSegment.from_wav(raw_audio_path)
    except Exception as e:
        raise RuntimeError(f"无法加载提取出的音频文件 {raw_audio_path}: {e}")
        
    total_duration_ms = len(original_audio)
    chunk_files = []
    chunks_dir = temp_dir / "audio_chunks"
    chunks_dir.mkdir(exist_ok=True)
    
    # 清理可能存在的旧音频块
    for item in chunks_dir.iterdir():
        item.unlink()

    num_chunks = -(-total_duration_ms // SUBTITLE_CHUNK_DURATION_MS)

    for i in range(num_chunks):
        start_time_ms = i * SUBTITLE_CHUNK_DURATION_MS
        end_time_ms = min((i + 1) * SUBTITLE_CHUNK_DURATION_MS, total_duration_ms)
        
        log_system_event("info", f"正在处理音频总块 {i+1}/{num_chunks}...", in_worker=True)
        
        audio_chunk = original_audio[start_time_ms:end_time_ms]
        temp_chunk_path = temp_dir / f"temp_chunk_{i}.wav"
        audio_chunk.export(temp_chunk_path, format="wav")
        
        processing_path = temp_chunk_path
        
        # 2.1 AI 降噪 (如果模型加载成功)
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
        
        # 2.2 VAD 语音检测
        try:
            from faster_whisper.audio import decode_audio
            from faster_whisper.vad import VadOptions, get_speech_timestamps
            
            vad_parameters = { "threshold": 0.38, "min_speech_duration_ms": 150, "max_speech_duration_s": 15.0, "min_silence_duration_ms": 1500, "speech_pad_ms": 500 }
            sampling_rate = 16000
            audio_data = decode_audio(str(processing_path), sampling_rate=sampling_rate)
            speech_timestamps = get_speech_timestamps(audio_data, vad_options=VadOptions(**vad_parameters))
            
            # 2.3 根据 VAD 结果从原始音频中精确切片
            for speech in speech_timestamps:
                relative_start_ms = int(speech["start"] / sampling_rate * 1000)
                relative_end_ms = int(speech["end"] / sampling_rate * 1000)
                absolute_start_ms = start_time_ms + relative_start_ms
                absolute_end_ms = start_time_ms + relative_end_ms
                
                final_chunk = original_audio[absolute_start_ms:absolute_end_ms]
                final_chunk_path = chunks_dir / f"chunk_{absolute_start_ms}.wav"
                final_chunk.export(str(final_chunk_path), format="wav")
                chunk_files.append({ "path": str(final_chunk_path), "start_ms": absolute_start_ms, "end_ms": absolute_end_ms })
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
    # 【修改后】增加了将请求参数存入任务字典的逻辑，以支持动态进度计算。
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

    # 初始化任务状态结构
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
            # --- START OF MODIFICATION ---
            # 约定一个内部键来存储原始参数，供进度模型使用
            "_internal_params": request_params,
            # --- END OF MODIFICATION ---
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

    # 返回体保持不变
    return jsonify({
        "task_id": task_id,
        "status_url": f"/api/tasks/{task_id}"
    }), 202

# --- (将下面的函数完整替换掉原来的版本) ---
@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task_status_endpoint(task_id):
    #
    # 【健壮版】获取任务状态，并处理僵尸/超时任务，确保总能发出“任务完成”信号。
    #
    with tasks_lock:
        task = tasks.get(task_id)

    if not task:
        # 如果任务不存在，这本身就是一个最终状态。
        # 我们返回 404，但可以加上完成信号头，
        # 以防协调器侧认为会话存在但Worker侧已清理。
        response = jsonify({"error": "未找到指定的 task_id", "status": "NOT_FOUND"})
        response.status_code = 404
        response.headers['X-Task-Completed'] = 'true'
        log_system_event("warning", f"查询不存在的任务 {task_id}，发送清理信号。")
        return response

    # --- START OF THE MODIFICATION ---
    
    # 定义任务的“最终状态”和“活动状态”
    terminal_states = {"SUCCESS", "FAILED", "PARTIAL_SUCCESS"}
    active_states = {"QUEUED", "RUNNING"}
    
    # 检查任务是否已经处于已知的最终状态
    current_status = task.get("status")
    if current_status in terminal_states:
        # 任务已正常结束，按原计划添加完成信号
        response = jsonify(task)
        response.headers['X-Task-Completed'] = 'true'
        return response

    # 如果任务不处于最终状态，则检查是否“僵死”
    if current_status in active_states:
        # 定义一个超时阈值，例如30分钟
        # updatedAt 应该在每次状态更新时被修改
        MAX_STUCK_DURATION = timedelta(minutes=300)
        try:
            # 解析任务的 updatedAt 时间戳
            updated_at_str = task.get("updatedAt", task.get("createdAt"))
            # 移除 'Z' 并处理毫秒部分
            updated_at_str = updated_at_str.replace('Z', '').split('.')[0]
            last_update_time = datetime.strptime(updated_at_str, '%Y-%m-%dT%H:%M:%S')

            if datetime.utcnow() - last_update_time > MAX_STUCK_DURATION:
                log_system_event("error", f"任务 {task_id} 状态为 {current_status} 已超过 {MAX_STUCK_DURATION} 未更新，判定为僵死。")
                
                # 主动将任务标记为失败
                task["status"] = "FAILED"
                task["error"] = {
                    "code": "TASK_TIMEOUT",
                    "message": f"Task was stuck in '{current_status}' state for too long."
                }
                # 更新任务状态，以便下次查询能看到失败信息
                with tasks_lock:
                    tasks[task_id] = task

                # 既然我们已经仲裁了它的最终状态，现在就可以发送“完成”信号了
                response = jsonify(task)
                response.status_code = 500 # 表示任务本身失败了
                response.headers['X-Task-Completed'] = 'true'
                return response
        except (ValueError, KeyError) as e:
            # 如果时间戳格式不正确或不存在，记录错误但继续
            log_system_event("warning", f"无法解析任务 {task_id} 的时间戳来检查超时: {e}")

    # 如果任务不处于最终状态，也不被认为是僵死，
    # 则正常返回当前状态，不带“完成”信号
    return jsonify(task)
    
    # --- END OF THE MODIFICATION ---

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
# 删了哦就安全了
def process_unified_task(task_data: dict, result_queue: multiprocessing.Queue, upload_queue: multiprocessing.Queue, subtitle_config: dict, ai_models: dict):
    """
    【进度计算最终修正版】此函数不再计算总进度。
    它只向主进程报告最原始的阶段性进度事件，由主进程进行权威计算。
    """
    task_id = task_data['task_id']
    params = task_data['params']
    temp_dir = Path(f"/kaggle/working/task_{task_id}")
    temp_dir.mkdir(exist_ok=True)
    
    # --- 新的、简化的状态更新函数 ---
    def _update_component_status(component, status=None, details=None, output=None, error_obj=None):
        payload = {'type': 'component_update', 'task_id': task_id, 'component': component, 'data': {}}
        if status: payload['data']['status'] = status
        if details: payload['data']['details'] = details
        if output: payload['data']['output'] = output
        if error_obj: payload['data']['error'] = error_obj
        if payload['data']:
            result_queue.put(payload)

    def _report_stage_progress(stage_key, stage_percent):
        """只报告阶段和其子进度，不计算总进度。"""
        payload = {
            'type': 'progress_event', 
            'task_id': task_id, 
            'stage': stage_key, 
            'percent': stage_percent # (0.0 to 1.0)
        }
        result_queue.put(payload)
    
    try:
        # --- 步骤 1 (下载文件) ---
        file_url = params['url']
        filename = unquote(file_url.split("/")[-1].split("?")[0] or f"file_{task_id}")
        local_file_path = temp_dir / filename

        # 检查是否需要下载（如果视频和字幕都不上传且不提取字幕，则跳过）
        needs_download = params.get("upload_video", True) or params.get("extract_subtitle", False)

        if needs_download:
            _update_component_status(component="video", status="RUNNING", details="开始下载文件...")
            _update_component_status(component="subtitle", status="RUNNING", details="等待视频下载...")
            _report_stage_progress('download', 0.0)
            
            with requests.get(file_url, stream=True, timeout=60) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                bytes_downloaded = 0
                with open(local_file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        if total_size > 0:
                            dl_percent = bytes_downloaded / total_size
                            _update_component_status(component="video", details=f"下载中 ({int(dl_percent*100)}%)...")
                            _report_stage_progress('download', dl_percent)
            _update_component_status(component="video", details="下载完成")
            _report_stage_progress('download', 1.0) # 报告下载阶段100%完成
        else:
             local_file_path = None

        # --- 步骤 2 (音频提取) ---
        raw_audio_path_for_subtitle = None
        if params["extract_subtitle"]:
            mime_type, _ = mimetypes.guess_type(local_file_path) if local_file_path and local_file_path.exists() else (None, None)
            if not (mime_type and mime_type.startswith("video")):
                _update_component_status(component="subtitle", status="SKIPPED", details="源文件不是视频格式")
                _report_stage_progress('subtitle_pipeline', 1.0) # 如果跳过，也报告此阶段100%完成
            else:
                try:
                    def sub_progress_callback(stage, details): _update_component_status(component="subtitle", details=details)
                    raw_audio_path_for_subtitle = extract_audio_with_ffmpeg(local_file_path, temp_dir, sub_progress_callback)
                except Exception as e:
                    _update_component_status(component="subtitle", status="FAILED", details=f"音频提取失败: {e}", error_obj={"code": "AUDIO_EXTRACTION_FAILED", "message": str(e)})
                    _report_stage_progress('subtitle_pipeline', 1.0) # 失败也报告此阶段100%完成
        else:
            _update_component_status(component="subtitle", status="SKIPPED", details="用户未请求提取")

        # --- 步骤 3 (并行派发任务) ---
        if params["upload_video"]:
            _update_component_status(component="video", status="RUNNING", details="已派发上传任务")
            _report_stage_progress('video_upload', 0.01) # 报告上传阶段开始
            upload_queue.put({
                'task_id': task_id, 'component': 'video', 'local_file_path': str(local_file_path),
                'filename_for_link': filename
            })
        else:
            _update_component_status(component="video", status="SKIPPED", details="用户未请求上传")
            _report_stage_progress('video_upload', 1.0) # 如果跳过，也报告此阶段100%完成
        
        if raw_audio_path_for_subtitle:
            try:
                # -- 字幕管线的子进度报告 --
                def subtitle_pipeline_progress_reporter(sub_stage_name, sub_stage_progress_percent):
                    # VAD占25%，AI处理占65%，上传占10%
                    if sub_stage_name == "preprocess":
                        stage_percent = 0.10 + (sub_stage_progress_percent * 0.25)
                    elif sub_stage_name == "ai_transcribe":
                        stage_percent = 0.35 + (sub_stage_progress_percent * 0.65)
                    else: # 其他情况不报告子进度
                        stage_percent = 0.10 # 停留在音频提取完成的阶段
                    
                    _report_stage_progress('subtitle_pipeline', stage_percent)
                
                def sub_progress_callback(stage, details): 
                    _update_component_status(component="subtitle", details=details)
                    if stage == 'subtitle_vad':
                        subtitle_pipeline_progress_reporter("preprocess", 1.0)
                    if stage == 'subtitle_transcribing':
                        try:
                            parts = details.split(" ")
                            if len(parts) > 2 and "/" in parts[1]:
                                done, total = map(int, parts[1].split('/'))
                                subtitle_pipeline_progress_reporter("ai_transcribe", done / total)
                        except: pass
                
                _report_stage_progress('subtitle_pipeline', 0.10) # 报告音频提取完成，字幕管线完成10%
                audio_chunks = preprocess_audio_for_subtitles(raw_audio_path_for_subtitle, temp_dir, sub_progress_callback, ai_models)
                
                srt_content = run_subtitle_extraction_pipeline(subtitle_config, audio_chunks, sub_progress_callback)
                if not srt_content: raise RuntimeError("未能生成有效的字幕内容。")
                
                _update_component_status(component="subtitle", output={"contentBase64": base64.b64encode(srt_content.encode('utf-8')).decode('utf-8')})
                
                if params["upload_subtitle"]:
                    _update_component_status(component="subtitle", status="RUNNING", details="已派发字幕上传任务")
                    _report_stage_progress('subtitle_upload', 0.01) # 这是一个新的阶段事件
                    srt_filename = local_file_path.stem + ".srt"
                    srt_path = temp_dir / srt_filename
                    with open(srt_path, "w", encoding="utf-8") as f: f.write(srt_content)
                    upload_queue.put({
                       'task_id': task_id, 'component': 'subtitle', 'local_file_path': str(srt_path),
                       'filename_for_link': srt_filename
                    })
                else:
                    _update_component_status(component="subtitle", status="SUCCESS", details="提取成功")
                    _report_stage_progress('subtitle_pipeline', 1.0)

            except Exception as e:
                _update_component_status(component="subtitle", status="FAILED", details=str(e), error_obj={"code": "SUBTITLE_PIPELINE_FAILED", "message": str(e)})
                _report_stage_progress('subtitle_pipeline', 1.0)
    except Exception as e:
        log_system_event("error", f"处理任务 {task_id} 时发生严重错误: {e}", in_worker=True)
        # 发送一个顶级的失败事件
        result_queue.put({'type': 'task_failed', 'task_id': task_id, 'error': {"code": "FATAL_ERROR", "message": str(e)}})
    finally:
        log_system_event("info", f"媒体处理进程 {task_id} 的主要工作已完成。", in_worker=True)

# =============================================================================
# --- 第 8 步: 多进程 Worker 与主程序 ---
# =============================================================================


# =============================================================================
# --- (新增方法 1/3) uploader_process_loop [全新] ---
# =============================================================================

# 这个新函数专门负责文件上传，运行在独立的进程中
def uploader_process_loop(upload_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue):
    """
    【进度计算最终修正版】上传工作进程，不再关心总进度。
    只向主进程报告其负责组件的原始上传进度事件。
    """
    log_system_event("info", "上传专用工作进程已启动。", in_worker=True)
    task_proxy_cache = {}
    proxy_manager = ProxyManager(subtitle_config_global.get("V2RAY_SUB_URL"))
    xray_process = None

    # start_xray_for_task 内部逻辑不变
    def start_xray_for_task(config):
        nonlocal xray_process
        if xray_process:
            xray_process.terminate()
            xray_process.wait()
        
        config_path = Path("/kaggle/working/xray_final_config.json")
        with open(config_path, 'w') as f: json.dump(config, f)
        
        xray_process = run_command(f"{proxy_manager.xray_path} -c {config_path}", "xray_upload.log")
        if not wait_for_port(proxy_manager.local_socks_port, host='127.0.0.1', timeout=10):
            log_system_event("error", "启动最优代理节点用于上传失败！", in_worker=True)
            return False
        return True

    # --- 新的、简化的状态更新和进度报告函数 ---
    def _update_component_status(task_id, component, status=None, details=None, output=None, error_obj=None):
        payload = {'type': 'component_update', 'task_id': task_id, 'component': component, 'data': {}}
        if status: payload['data']['status'] = status
        if details: payload['data']['details'] = details
        if output: payload['data']['output'] = output
        if error_obj: payload['data']['error'] = error_obj
        if payload['data']:
            result_queue.put(payload)
            
    def _report_upload_progress(task_id, component, upload_percent):
        """报告上传进度的原始事件。"""
        stage_key = 'video_upload' if component == 'video' else 'subtitle_upload'
        payload = {
            'type': 'progress_event',
            'task_id': task_id,
            'stage': stage_key,
            'percent': upload_percent # (0.0 to 1.0)
        }
        result_queue.put(payload)
        
    try:
        while True:
            try:
                upload_task_data = upload_queue.get()
                if upload_task_data is None: break

                task_id = upload_task_data['task_id']
                component = upload_task_data['component']
                local_file_path_str = upload_task_data['local_file_path']
                
                # V2: 从主进程传递 API 客户端基础URL，以支持测速
                # 在 V3 中这个信息现在从 upload_task_data 中来
                api_client_base_url = upload_task_data.get('api_client_base_url', f"http://127.0.0.1:{MIXFILE_LOCAL_PORT}")

                # --- 按需测速逻辑 (保持不变) ---
                if task_id not in task_proxy_cache:
                    proxies_for_task, config_for_task = proxy_manager.get_best_proxy_for_upload(api_client_base_url)
                    task_proxy_cache[task_id] = {'proxies': proxies_for_task, 'config': config_for_task}
                    if config_for_task:
                        if not start_xray_for_task(config_for_task):
                            task_proxy_cache[task_id]['proxies'] = None
                    elif xray_process:
                        xray_process.terminate()
                        xray_process.wait()
                        xray_process = None

                current_proxies = task_proxy_cache[task_id]['proxies']

                try:
                    filename_for_link = upload_task_data['filename_for_link']
                    
                    api_client = MixFileCLIClient(base_url=api_client_base_url, proxies=current_proxies)
                    log_system_event("info", f"[上传进程] [{component}] 开始处理上传任务 (代理: {'启用' if current_proxies else '禁用'})...", in_worker=True)
                    _update_component_status(task_id, component, status="RUNNING", details="正在上传 (0%)...")
                    
                    # --- 进度回调逻辑修改 ---
                    def progress_callback(bytes_uploaded, total_bytes):
                        if total_bytes > 0:
                            percent_float = bytes_uploaded / total_bytes
                            # 只报告原始事件
                            _report_upload_progress(task_id, component, percent_float)
                            # details 更新可以保留，因为它对UI友好
                            _update_component_status(task_id, component, details=f"正在上传 ({int(percent_float * 100)}%)")

                    response = api_client.upload_file(local_file_path_str, progress_callback=progress_callback)
                    
                    if isinstance(response, requests.Response) and response.ok:
                        share_code = response.text.strip()
                        output_data = {"shareCode": share_code}
                        _update_component_status(task_id, component, "SUCCESS", "上传成功", output=output_data)
                        log_system_event("info", f"✅ [上传进程] [{component}] 上传成功。", in_worker=True)
                    else:
                        error_msg = f"HTTP Error"
                        if isinstance(response, requests.Response):
                            error_msg = f"HTTP {response.status_code}: {response.text}"
                        elif isinstance(response, tuple):
                            error_msg = f"请求失败 (状态码 {response[0]}): {response[1]}"
                        raise RuntimeError(error_msg)

                except Exception as e:
                    log_system_event("error", f"❌ [上传进程] [{component}] 上传任务发生严重错误: {e}", in_worker=True)
                    _update_component_status(task_id, component, "FAILED", f"上传失败: {e}", error_obj={"code": "UPLOAD_FAILED", "message": str(e)})

                finally:
                    # 文件清理和代理缓存清理逻辑 (保持不变)
                    try: Path(local_file_path_str).unlink(missing_ok=True)
                    except OSError: pass
                    
                    is_last_upload = False
                    with tasks_lock: # 需要访问主进程的全局 tasks 来判断
                        if task_id in tasks:
                             # 如果字幕组件被跳过，则视频上传就是最后一个
                            if component == 'video' and tasks[task_id]['results']['subtitle']['status'] in ('SKIPPED', 'FAILED'):
                                is_last_upload = True
                            # 如果字幕组件没被跳过，则字幕上传是最后一个
                            elif component == 'subtitle':
                                is_last_upload = True

                    if is_last_upload:
                        log_system_event("info", f"任务 {task_id} 上传环节完成，清理代理缓存。", in_worker=True)
                        if task_id in task_proxy_cache: del task_proxy_cache[task_id]
                        if xray_process:
                           xray_process.terminate()
                           xray_process.wait()
                           xray_process = None

            except QueueEmpty: continue
            except Exception as e:
                log_system_event("critical", f"上传专用工作进程循环发生内部错误: {e}", in_worker=True)
    finally:
        if xray_process:
            xray_process.terminate()
            xray_process.wait()
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
    【进度计算最终修正版】结果处理器。
    这是唯一的、权威的进度计算中心。它监听来自子进程的原始事件，
    并使用内置的 TaskProgressModel 将其转化为准确的总进度。
    """
    log_system_event("info", "结果处理线程已启动。")
    
    task_progress_models = {}

    class TaskProgressModel:
        def __init__(self, params):
            self.params = params
            self.completed_weight = 0
            self._completed_stages = set()
            self.last_known_stage_progress = {}

            # -- 权重定义 --
            # 权重现在更细粒度，字幕上传独立出来
            PROGRESS_WEIGHTS = {
                'download': 20,
                'video_upload': 40,
                'subtitle_pipeline': 30, # 音频提取、VAD、AI
                'subtitle_upload': 10,
            }

            self.active_weights = {}
            if self.params.get("upload_video", True):
                self.active_weights['video_upload'] = PROGRESS_WEIGHTS['video_upload']
            
            if self.params.get("extract_subtitle", False):
                self.active_weights['subtitle_pipeline'] = PROGRESS_WEIGHTS['subtitle_pipeline']
                if self.params.get("upload_subtitle", False):
                    self.active_weights['subtitle_upload'] = PROGRESS_WEIGHTS['subtitle_upload']

            # 只有在有需要处理的组件时，才加上下载的权重
            if self.active_weights:
                self.active_weights['download'] = PROGRESS_WEIGHTS['download']
            
            self.total_active_weight = sum(self.active_weights.values())
            if self.total_active_weight == 0: self.total_active_weight = 100
        
        def _update_stage_progress(self, stage_key: str, percent: float):
            # 确保进度不会回退
            if percent >= self.last_known_stage_progress.get(stage_key, 0.0):
                self.last_known_stage_progress[stage_key] = percent
                if percent >= 1.0:
                    self._mark_stage_as_completed(stage_key)

        def _mark_stage_as_completed(self, stage_key: str):
            if stage_key in self.active_weights and stage_key not in self._completed_stages:
                self.completed_weight += self.active_weights[stage_key]
                self._completed_stages.add(stage_key)
        
        def get_total_progress(self) -> int:
            current_progress_contribution = 0
            for stage, weight in self.active_weights.items():
                if stage not in self._completed_stages:
                    stage_percent = self.last_known_stage_progress.get(stage, 0.0)
                    current_progress_contribution += weight * stage_percent
            
            total_progress = self.completed_weight + current_progress_contribution
            normalized_progress = int((total_progress / self.total_active_weight) * 100)
            return min(normalized_progress, 100) # 确保不超过100

        def update_from_event(self, event_type: str, component: str, data: dict):
            # 当组件更新最终状态时，也更新进度模型的完成状态
            status = data.get("status")
            if status not in ("PENDING", "RUNNING", None): # (SUCCESS, FAILED, SKIPPED)
                if component == "video":
                    self._update_stage_progress("video_upload", 1.0)
                elif component == "subtitle":
                    self._update_stage_progress("subtitle_pipeline", 1.0)
                    if self.params.get("upload_subtitle", False) and status != 'SKIPPED':
                        # 如果字幕上传失败或成功，都算这个阶段结束了
                        self._update_stage_progress("subtitle_upload", 1.0)

    while True:
        try:
            result_data = result_queue.get(timeout=3600)
            task_id = result_data['task_id']
            data_type = result_data.get('type')

            with tasks_lock:
                if task_id not in tasks: continue
                task = tasks[task_id]

                if task_id not in task_progress_models:
                    task_params = task.get('_internal_params')
                    if task_params:
                        task_progress_models[task_id] = TaskProgressModel(task_params)
                
                progress_model = task_progress_models.get(task_id)

                task['updatedAt'] = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
                
                # --- 事件处理与进度计算 ---
                if data_type == 'progress_event' and progress_model:
                    stage = result_data['stage']
                    percent = result_data['percent']
                    progress_model._update_stage_progress(stage, percent)
                
                elif data_type == 'component_update':
                    component = result_data['component']
                    data = result_data['data']
                    if component in task['results']:
                        # 合并 output
                        if 'output' in data and isinstance(task['results'][component].get('output'), dict):
                            task['results'][component]['output'].update(data['output'])
                            del data['output']
                        # 更新 status, details 等
                        task['results'][component].update(data)
                        if progress_model:
                            progress_model.update_from_event(data_type, component, data)

                elif data_type == 'task_failed':
                    task['status'] = 'FAILED'
                    task['error'] = result_data.get('error')
                    task['progress'] = 100
                
                # 只有在收到非终结性事件时才更新进度
                if task['status'] not in ["SUCCESS", "FAILED", "PARTIAL_SUCCESS"]:
                    task['status'] = "RUNNING"
                    if progress_model:
                        task['progress'] = progress_model.get_total_progress()

                # --- 任务最终状态评估 (逻辑基本不变) ---
                video_status = task['results']['video']['status']
                subtitle_status = task['results']['subtitle']['status']
                is_finished = "PENDING" not in (video_status, subtitle_status) and "RUNNING" not in (video_status, subtitle_status)

                if is_finished and task['status'] not in ["SUCCESS", "FAILED", "PARTIAL_SUCCESS"]:
                    # (这部分逻辑与之前一样，只是确保在最终状态评估前设置进度为100%)
                    log_system_event("info", f"任务 {task_id} 所有组件已完成，正在进行最终状态评估。")
                    task['progress'] = 100
                    
                    for comp in task['results']:
                        if task['results'][comp]['status'] == 'RUNNING':
                            task['results'][comp]['status'] = 'FAILED' #...

                    active_statuses = [s for s in (task['results']['video']['status'], task['results']['subtitle']['status']) if s != "SKIPPED"]
                    
                    if not active_statuses: task['status'] = "SUCCESS"
                    elif all(s == "SUCCESS" for s in active_statuses): task['status'] = "SUCCESS"
                    elif "FAILED" in active_statuses:
                        task['status'] = "PARTIAL_SUCCESS" if "SUCCESS" in active_statuses else "FAILED"
                    else: task['status'] = "SUCCESS"

                    log_system_event("info", f"任务 {task_id} 最终状态被设置为: {task['status']}")
                    
                    if task_id in task_progress_models: del task_progress_models[task_id]

                    temp_dir_to_clean = Path(f"/kaggle/working/task_{task_id}")
                    if temp_dir_to_clean.exists():
                        log_system_event("info", f"任务 {task_id} 已结束，准备清理临时目录...")
                        try:
                            shutil.rmtree(temp_dir_to_clean)
                            log_system_event("info", f"✅ 成功清理任务 {task_id} 的临时目录。")
                        except Exception as e:
                            log_system_event("error", f"❌ 清理任务 {task_id} 的临时目录失败: {e}")

        except QueueEmpty: continue
        except Exception as e: log_system_event("error", f"结果处理线程发生严重错误: {e}")

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
        wifi = speedtest.Speedtest()
        install_other_cmd = "pip install -q pydantic pydub faster-whisper@https://github.com/SYSTRAN/faster-whisper/archive/refs/heads/master.tar.gz denoiser google-generativeai requests psutil speedtest-cli"
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