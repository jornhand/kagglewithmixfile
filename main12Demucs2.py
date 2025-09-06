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
from concurrent.futures import ThreadPoolExecutor
# --- 多进程与队列 ---
import multiprocessing
from queue import Empty as QueueEmpty

# --- Web 框架与 HTTP 客户端 ---
from flask import Flask, request, jsonify, Response
import requests



# =============================================================================
# --- 新增：高级音频处理库导入 ---
# =============================================================================
try:
    from demucs.separate import S
    import tensorflow as tf
    import tensorflow_hub as hub
    from speechbrain.pretrained import SepformerLSTMMaskNet as SpeechEnhancer
except ImportError as e:
    print(f"[警告] 预加载高级音频处理库失败: {e}。这些库将在运行时安装。")
    S, tf, hub, SpeechEnhancer = None, None, None, None # 保持单行赋值
# =============================================================================



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
ENCRYPTED_FRP_CONFIG = "gAAAAABouqe8kDRGZxX7I43TbmNqmy2TZNRJK0f5GqrXwKwW0VR3TtfyfFmDvHCtPbfMzcBkT1qgfMPzhPWDuHRmwibWSmVOgi4nZe_J1TTmLAWMSzQOieGMWE2LXRIIKbf-dLMCnjPDKRkLmZBuWtreN5QgZC41Px3hRU7pxqw51edAkOmvZiE7nf9G0AQ0IvCAR9WHMDg6"

# 第二个字符串用于字幕服务 (Gemini API 密钥等)
ENCRYPTED_SUBTITLE_CONFIG = "gAAAAABouqgGsJh7-osSg3oc2OhtcGotCfGufGZLgeV_4hF6mr5qrhh2wKCjalhFRuPdeUPMNHWqz5CRju55o4CPcr_Deprf-hktkuQ9sDhtRI0mWMkqqwlqPO28AClTO-q0bKXZ74BEPcTCXPVeHZaG_gTolEMblHXXuNK7Mm3VlJV7S5PE1WuvjG4uhne7UtbzYWx5TBE46df09vAxEXb7Tyo05B2jSjkm8NVM3JRmOfSWFpvv9r-J7n3ZCyPCHDj6hc0-6IBkqnX4il41zdgovtxXVugNeploPQylxPI240L1nm6zRKfSlplkNhZ3k1reH3LjDyY1zfp5aEhNKqQXBUrQY_4c--Km2LbQml9BbjRhOnJmUF2uo7Aa7c8mZUM4-jWNDU8KD0dCJFrWkeUQoW7bPlicrw==" # 示例，请替换

# -- C. 本地服务与端口配置 --
MIXFILE_LOCAL_PORT = 4719
FLASK_API_LOCAL_PORT = 5000
MIXFILE_REMOTE_PORT = 20000  # 映射到公网的 MixFile 端口
FLASK_API_REMOTE_PORT = 20001  # 映射到公网的 Flask API 端口

# -- D. Killer API & 进程管理配置 --
KILLER_API_SHUTDOWN_TOKEN = "change-this-to-a-secure-random-string" # !!! 强烈建议修改 !!!
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
    update_status_callback: callable
) -> list[dict]:
    
    # 【核心修复】：在子进程函数内部，强制重新导入可能在启动时为None的模块
    # 这确保了即使父进程中这些变量是None，子进程也能获取到正确的模块
    from demucs.separate import S
    import tensorflow_hub as hub
    import tensorflow as tf
    from speechbrain.pretrained import SepformerLSTMMaskNet as SpeechEnhancer
    from faster_whisper.audio import decode_audio
    from faster_whisper.vad import VadOptions, get_speech_timestamps

    # --- STAGE 0: 初始化模型 ---
    update_status_callback(stage="subtitle_init_models", details="正在初始化高级音频处理模型...")

    # 加载 SpeechBrain 语音增强模型
    try:
        speech_enhancer = SpeechEnhancer.from_hparams(
            source="speechbrain/sepformer-dns4-16k-enhancement",
            savedir=str(temp_dir / "pretrained_models/sepformer-dns4-16k-enhancement"),
            run_opts={"device": "cuda"} # 明确指定设备
        )
        log_system_event("info", "✅ SpeechBrain 语音增强模型加载成功。", in_worker=True)
    except Exception as e:
        log_system_event("warning", f"加载 SpeechBrain 模型失败，将跳过语音增强。错误: {e}", in_worker=True)
        speech_enhancer = None
        
    # 加载 YAMNet 声音事件检测模型
    try:
        yamnet_model = hub.load('https://tfhub.dev/google/yamnet/1')
        yamnet_class_map_path = yamnet_model.class_map_path().numpy().decode('utf-8')
        yamnet_class_names = [name.strip('"') for name in open(yamnet_class_map_path).read().strip().split('\n')]
        log_system_event("info", "✅ YAMNet 声音事件检测模型加载成功。", in_worker=True)
    except Exception as e:
        log_system_event("warning", f"加载 YAMNet 模型失败，将跳过内容甄别。错误: {e}", in_worker=True)
        yamnet_model = None

    # --- STAGE 1: 提取与粗分离 (Demucs) ---
    update_status_callback(stage="subtitle_extract_audio", details="正在提取原始音频...")
    raw_audio_path = temp_dir / "raw_audio.wav"
    try:
        command = [ "ffmpeg", "-i", str(video_path), "-ac", "2", "-ar", "44100", "-vn", "-y", "-loglevel", "error", str(raw_audio_path) ]
        subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e: raise RuntimeError(f"FFmpeg 提取音频失败: {e.stderr}")

    update_status_callback(stage="subtitle_demucs_separation", details="正在使用 Demucs 分离人声...")
    demucs_args = [ "-n", "htdemucs_ft", "--two-stems", "vocals", "-d", "cuda", "--out", str(temp_dir / "demucs_output"), str(raw_audio_path) ]
    old_stdout, sys.stdout = sys.stdout, StringIO()
    try:
        S.sub_run(demucs_args)
    except Exception as demucs_error:
        # 增加对 Demucs 失败的捕获，防止整个任务中断
        log_system_event("error", f"Demucs 执行失败: {demucs_error}", in_worker=True)
    finally:
        sys.stdout = old_stdout
    
    vocals_path = temp_dir / "demucs_output" / "htdemucs_ft" / raw_audio_path.stem / "vocals.wav"
    if not vocals_path.exists():
        log_system_event("warning", "Demucs未能分离人声，将使用原始音频。", in_worker=True)
        processing_audio_path = raw_audio_path
    else:
        log_system_event("info", "✅ Demucs 成功分离人声。", in_worker=True)
        processing_audio_path = vocals_path
        
    # --- STAGE 2: 统一格式并进行精细语音增强 (SpeechBrain) ---
    update_status_callback(stage="subtitle_speech_enhancement", details="正在进行深度语音增强...")
    # 转换到模型所需的 16kHz 单声道
    enhanced_audio_path = temp_dir / "enhanced_audio.wav"
    ffmpeg_resample_cmd = [ "ffmpeg", "-i", str(processing_audio_path), "-ac", "1", "-ar", "16000", "-y", "-loglevel", "error", str(enhanced_audio_path)]
    subprocess.run(ffmpeg_resample_cmd, check=True)
    
    if speech_enhancer:
        try:
            # SpeechBrain v0.5.16 的 API 需要 tensor 输入
            waveform, sr = torchaudio.load(str(enhanced_audio_path))
            enhanced_wav = speech_enhancer.enhance_batch(waveform.cuda(), lengths=torch.tensor([1.0]).cuda())
            torchaudio.save(str(enhanced_audio_path), enhanced_wav.squeeze(0).cpu(), 16000)
            log_system_event("info", "✅ 语音增强处理完成。", in_worker=True)
        except Exception as e:
            log_system_event("warning", f"SpeechBrain 增强失败，将使用未增强的音频。错误: {e}", in_worker=True)

    # --- STAGE 3: 内容甄别 (VAD + YAMNet) ---
    update_status_callback(stage="subtitle_content_filtering", details="正在检测语音并过滤非说话声...")
    vad_parameters = { "threshold": 0.4, "min_speech_duration_ms": 250, "max_speech_duration_s": 15.0, "min_silence_duration_ms": 1000, "speech_pad_ms": 150 }
    audio_data = decode_audio(str(enhanced_audio_path), sampling_rate=16000)
    speech_timestamps = get_speech_timestamps(audio_data, vad_options=VadOptions(**vad_parameters))

    original_audio = AudioSegment.from_wav(enhanced_audio_path)
    chunk_files = []
    chunks_dir = temp_dir / "audio_chunks"
    chunks_dir.mkdir(exist_ok=True)
    
    for speech in speech_timestamps:
        start_ms = int(speech["start"] / 16000 * 1000)
        end_ms = int(speech["end"] / 16000 * 1000)
        segment_audio = original_audio[start_ms:end_ms]
        
        is_speech_flag = True
        
        if yamnet_model and len(segment_audio) > 0:
            segment_samples = np.array(segment_audio.get_array_of_samples()).astype(np.float32) / (1 << 15)
            scores, embeddings, spectrogram = yamnet_model(segment_samples)
            scores_np = scores.numpy().mean(axis=0)
            top5_indices = np.argsort(scores_np)[-5:][::-1]
            top_classes = [yamnet_class_names[i] for i in top5_indices]

            undesired_sounds = {"Shout", "Yell", "Groan", "Sigh", "Screaming", "Crying, sobbing"}
            
            # 改进的甄别逻辑：如果最高分的分类是不期望的声音，
            # 并且"Speech"不在前两名的分类中，则过滤掉。
            if top_classes[0] in undesired_sounds and "Speech" not in top_classes[:2]:
                log_system_event("info", f"过滤掉片段 {start_ms}ms - {end_ms}ms (检测为: {top_classes[0]})", in_worker=True)
                is_speech_flag = False

        if is_speech_flag:
            final_chunk_path = chunks_dir / f"chunk_{start_ms}.wav"
            segment_audio.export(str(final_chunk_path), format="wav")
            chunk_files.append({ "path": str(final_chunk_path), "start_ms": start_ms, "end_ms": end_ms })

    log_system_event("info", f"音频纯化处理完成，总共获得 {len(chunk_files)} 个有效说话片段。", in_worker=True)
    return chunk_files

# --- D. AI 交互与并发调度 ---

def _process_subtitle_batch_with_ai(
    chunk_group: list[dict],
    group_index: int,
    api_key: str,
    gemini_endpoint_prefix: str,
    prompt_api_url: str  # <--- 核心修改：不再接收提示词本身，而是接收API的URL
) -> list[dict]:
    #
    # 处理单个批次的音频块，调用 Gemini API 获取字幕。
    # 这是一个内部函数，由主调度器调用。
    #
    thread_local_srt_list = []
    log_system_event("info", f"[字幕任务 {group_index+1}] 已启动...", in_worker=True)
    try:
        # 【新逻辑】：在每个批次处理开始时，独立获取动态提示词
        system_instruction, prompt_for_task = get_dynamic_prompts(prompt_api_url)

        # 1. 构建 REST API 请求体 (payload) - 后续逻辑保持不变
        model_name = "gemini-2.5-flash"
        
        generate_url = f"{gemini_endpoint_prefix.rstrip('/')}/v1beta/models/{model_name}:generateContent"
        
        headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
        
        parts = [{"text": prompt_for_task}]
        for chunk in chunk_group:
            encoded_data = read_and_encode_file_base64(chunk["path"])
            if not encoded_data:
                log_system_event("error", f"[字幕任务 {group_index+1}] 文件 {chunk['path']} 编码失败，跳过此文件。", in_worker=True)
                continue
            parts.append({"text": f"[AUDIO_INFO] {chunk['start_ms']} --> {chunk['end_ms']}"})
            parts.append({"inlineData": {"mime_type": "audio/wav", "data": encoded_data}})
        
        if len(parts) <= 1:
            log_system_event("error", f"[字幕任务 {group_index+1}] 整个批次均无法编码，放弃。", in_worker=True)
            return []

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

        # 2. 发送请求并实现全面的重试逻辑
        log_system_event("info", f"[字幕任务 {group_index+1}] 数据准备完毕，正在调用 Gemini API at {generate_url}...", in_worker=True)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(generate_url, headers=headers, json=payload, timeout=1000)
                
                if response.status_code in [429, 500, 503, 504]:
                    log_system_event("warning", f"[字幕任务 {group_index+1}] 遇到可重试的 API 错误 (HTTP {response.status_code}, 尝试 {attempt + 1}/{max_retries})。", in_worker=True)
                    if attempt < max_retries - 1:
                        wait_time = 5 * (2 ** attempt)
                        log_system_event("info", f"{wait_time}秒后将自动重试...", in_worker=True)
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()

                response.raise_for_status()
                response_data = response.json()
                
                candidates = response_data.get("candidates")
                if not candidates:
                    raise ValueError(f"API响应中缺少 'candidates' 字段。响应: {response.text}")

                content = candidates[0].get("content")
                if not content:
                    finish_reason = candidates[0].get("finishReason", "未知")
                    safety_ratings = candidates[0].get("safetyRatings", [])
                    raise ValueError(f"API响应内容为空(可能被拦截)。原因: {finish_reason}, 安全评级: {safety_ratings}")

                json_text = content.get("parts", [{}])[0].get("text")
                if json_text is None:
                    raise ValueError(f"API响应的 'parts' 中缺少 'text' 字段。响应: {response.text}")

                parsed_result = BatchTranscriptionResult.model_validate_json(json_text)
                subtitles_count = len(parsed_result.subtitles)
                log_system_event("info", f"✅ [字幕任务 {group_index+1}] 成功！获得 {subtitles_count} 条字幕。", in_worker=True)
                
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
                
                return thread_local_srt_list

            except requests.exceptions.RequestException as e:
                log_system_event("warning", f"[字幕任务 {group_index+1}] 网络错误 (尝试 {attempt + 1}/{max_retries}): {e}", in_worker=True)
                if attempt < max_retries - 1: time.sleep(5 * (2 ** attempt))
            except pydantic.ValidationError as e:
                log_system_event("error", f"[字幕任务 {group_index+1}] AI返回的JSON格式验证失败，放弃。错误: {e}", in_worker=True)
                return []
            except Exception as e:
                log_system_event("error", f"[字幕任务 {group_index+1}] 不可重试的错误，放弃。错误: {e}", in_worker=True)
                return []
        
        raise RuntimeError(f"批次 {group_index+1} 在 {max_retries} 次尝试后仍然失败。")

    except Exception as e:
        log_system_event("error", f"[字幕任务 {group_index+1}] 发生严重错误: {e}", in_worker=True)
    
    return thread_local_srt_list

def run_subtitle_extraction_pipeline(subtitle_config: dict, chunk_files: list[dict], update_status_callback: callable) -> str:
    #
    # 主调度器，负责并发处理所有音频批次并生成最终的 SRT 内容。
    #
    
    api_keys = subtitle_config.get('GEMINI_API_KEYS', [])
    prompt_api_url = subtitle_config.get('PROMPT_API_URL', '')
    gemini_endpoint_prefix = subtitle_config.get('GEMINI_API_ENDPOINT_PREFIX', '')
    
    if not all([api_keys, prompt_api_url, gemini_endpoint_prefix]):
        raise ValueError("字幕配置中缺少 'GEMINI_API_KEYS', 'PROMPT_API_URL', 或 'GEMINI_API_ENDPOINT_PREFIX'。")
    
    # 【核心修改】：移除此处的 get_dynamic_prompts 调用
    # system_instruction, prompt_for_task = get_dynamic_prompts(prompt_api_url)

    total_chunks = len(chunk_files)
    if total_chunks == 0:
        log_system_event("warning", "没有检测到有效的语音片段，无法生成字幕。", in_worker=True)
        return ""

    chunk_groups = [chunk_files[i:i + SUBTITLE_BATCH_SIZE] for i in range(0, total_chunks, SUBTITLE_BATCH_SIZE)]
    num_groups = len(chunk_groups)
    log_system_event("info", f"已将语音片段分为 {num_groups} 个批次，准备并发处理。", in_worker=True)
    update_status_callback(stage="subtitle_transcribing", details=f"准备处理 {num_groups} 个字幕批次...")
    
    all_srt_blocks = []
    
    with ThreadPoolExecutor(max_workers=SUBTITLE_CONCURRENT_REQUESTS) as executor:
        futures = []
        delay_between_submissions = 60.0 / SUBTITLE_REQUESTS_PER_MINUTE
        
        for i, group in enumerate(chunk_groups):
            api_key_for_thread = api_keys[i % len(api_keys)]
            future = executor.submit(
                _process_subtitle_batch_with_ai,
                group,
                i,
                api_key_for_thread,
                gemini_endpoint_prefix,
                prompt_api_url  # <--- 核心修改：传递 URL 而不是提示词内容
            )
            futures.append(future)
            if i < num_groups - 1:
                time.sleep(delay_between_submissions)
        
        for i, future in enumerate(futures):
            try:
                result = future.result()
                if result:
                    all_srt_blocks.extend(result)
                update_status_callback(stage="subtitle_transcribing", details=f"已完成 {i+1}/{num_groups} 个字幕批次...")
            except Exception as e:
                log_system_event("error", f"一个字幕线程任务在获取结果时发生错误: {e}", in_worker=True)

    log_system_event("info", "所有并发任务处理完成，正在整合字幕...", in_worker=True)
    
    all_srt_blocks.sort(key=lambda x: x["start_ms"])
    final_srt_lines = [f"{i + 1}\n{block['srt_line']}" for i, block in enumerate(all_srt_blocks)]
    
    return "\n".join(final_srt_lines)

# =============================================================================
# --- 第 5 步: MixFileCLI 客户端 ---
# =============================================================================

class MixFileCLIClient:
    # 一个简单的用于与 MixFileCLI 后端 API 交互的客户端。
    def __init__(self, base_url: str):
        if not base_url.startswith("http"):
            raise ValueError("Base URL 必须以 http 或 https 开头")
        self.base_url = base_url
        self.session = requests.Session()

    def _make_request(self, method: str, url: str, **kwargs):
        # 统一的请求发送方法，包含错误处理。
        try:
            # 确保请求不会被缓存
            headers = kwargs.get('headers', {})
            headers.update({'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
            kwargs['headers'] = headers
            
            response = self.session.request(method, url, timeout=300, **kwargs)
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
        # 上传单个文件并获取分享码。
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
        file_size = os.path.getsize(local_file_path)

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

        with open(local_file_path, 'rb') as f:
            return self._make_request("PUT", upload_url, data=file_reader_generator(f))

# =============================================================================
# --- 第 6 步: 统一的 Flask API 服务 (主进程) ---
# =============================================================================

# --- A. 应用初始化与任务管理 ---
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

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
    
    # 从请求中提取参数，并设置默认值
    request_params = {
        "url": data["url"],
        "extract_subtitle": data.get("extract_subtitle", False),
        "upload_video": data.get("upload_video", True),
        "upload_subtitle": data.get("upload_subtitle", False),
    }

    # 初始化任务状态
    with tasks_lock:
        tasks[task_id] = {
            "task_id": task_id,
            "status": "pending",
            "stage": "queue",
            "details": "任务已创建，等待处理",
            "progress": 0,
            "result": None
        }

    # 【核心修改】将任务数据放入队列，由子进程处理
    task_data = {
        'task_id': task_id,
        'params': request_params,
        'subtitle_config': subtitle_config_global, # 传递必要的配置
        'api_client_base_url': api_client.base_url,
        'frp_server_addr': FRP_SERVER_ADDR # 传递FRP地址用于构建链接
    }
    TASK_QUEUE.put(task_data)
    
    log_system_event("info", f"已创建新任务 {task_id} 并推入处理队列。")

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
        "status": "ok",
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

def process_unified_task(task_data: dict, result_queue: multiprocessing.Queue):
    #
    # 处理一个完整的媒体任务，包含下载、字幕提取和上传等步骤。
    # 这个函数实现了方案2的核心逻辑：子任务的独立失败处理。
    #
    
    # --- 1. 初始化 ---
    task_id = task_data['task_id']
    params = task_data['params']
    subtitle_config = task_data['subtitle_config']
    api_client_base_url = task_data['api_client_base_url']
    frp_server_addr = task_data['frp_server_addr']
    
    # 在子进程中重新创建api_client
    local_api_client = MixFileCLIClient(base_url=api_client_base_url)

    temp_dir = Path(f"/kaggle/working/task_{task_id}")
    temp_dir.mkdir(exist_ok=True)
    
    # 定义一个内部函数来更新任务状态，通过队列发送给主进程
    def _update_status(status, stage, details, progress=None):
        update_data = {
            'type': 'status_update',
            'task_id': task_id,
            'payload': {
                'status': status,
                'stage': stage,
                'details': details,
            }
        }
        if progress is not None:
            update_data['payload']['progress'] = progress
        result_queue.put(update_data)

    
    # 初始化最终结果结构
    final_result = {
        "video_file": {"status": "pending"},
        "subtitle_file": {"status": "pending"}
    }

    try:
        # --- 2. 子任务: 下载源文件 ---
        _update_status("running", "download", "准备从 URL 下载文件...", 0)
        
        file_url = params['url']
        # 从URL猜测文件名，并进行清理
        filename_encoded = file_url.split("/")[-1].split("?")[0] or f"downloaded_file_{task_id}"
        filename = unquote(filename_encoded)
        local_file_path = temp_dir / filename
        final_result["video_file"]["filename"] = filename

        bytes_downloaded = 0
        with requests.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(local_file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    if total_size > 0:
                        progress = round((bytes_downloaded / total_size) * 100, 2)
                        _update_status("running", "download", f"已下载 {bytes_downloaded}/{total_size} 字节", progress)
        
        # --- 3. 子任务: 媒体类型验证 ---
        _update_status("running", "validate", "正在验证文件类型...", 100)
        is_video = False
        mime_type, _ = mimetypes.guess_type(local_file_path)
        if mime_type and mime_type.startswith("video"):
            is_video = True
            log_system_event("info", f"文件 {filename} 被识别为视频 ({mime_type})。", in_worker=True)
        else:
            log_system_event("warning", f"文件 {filename} 不是标准视频格式 ({mime_type})。", in_worker=True)

        # --- 4. 子任务: 字幕提取 ---
        srt_content = None
        if params["extract_subtitle"]:
            if is_video:
                try:
                    _update_status("running", "subtitle_extraction", "字幕提取流程已启动...", 0)
                    # 调用完整的字幕提取管道
                    # 创建一个局部lambda，因为它不能被pickle传递
                    update_callback_for_sub = lambda stage, details: _update_status("running", stage, details)
                    audio_chunks = preprocess_audio_for_subtitles(local_file_path, temp_dir, update_callback_for_sub)
                    srt_content = run_subtitle_extraction_pipeline(subtitle_config, audio_chunks, update_callback_for_sub)
                    
                    if srt_content:
                        srt_filename = local_file_path.stem + ".srt"
                        final_result["subtitle_file"]["filename"] = srt_filename
                        final_result["subtitle_file"]["status"] = "success"
                        
                        # 返回Base64编码的SRT文件
                        srt_base64 = base64.b64encode(srt_content.encode('utf-8')).decode('utf-8')
                        final_result["subtitle_file"]["content_base64"] = srt_base64
                        
                        # 将SRT文件保存到本地以备上传
                        with open(temp_dir / srt_filename, "w", encoding="utf-8") as f:
                            f.write(srt_content)
                    else:
                        raise RuntimeError("未检测到有效语音，无法生成字幕。")

                except Exception as e:
                    log_system_event("error", f"任务 {task_id} 的字幕提取失败: {e}", in_worker=True)
                    final_result["subtitle_file"]["status"] = "failed"
                    final_result["subtitle_file"]["details"] = f"字幕提取失败: {str(e)}"
            else:
                final_result["subtitle_file"]["status"] = "skipped"
                final_result["subtitle_file"]["details"] = "源文件不是有效的视频格式"
        else:
            final_result["subtitle_file"]["status"] = "skipped"
            final_result["subtitle_file"]["details"] = "未请求提取字幕"

        # --- 5. 子任务: 文件上传 (使用并发) ---
        _update_status("running", "uploading", "准备上传文件到 MixFile...", 0)
        
        upload_tasks = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            # 提交视频上传任务
            if params["upload_video"]:
                upload_tasks.append(executor.submit(local_api_client.upload_file, str(local_file_path)))
            else:
                final_result["video_file"]["status"] = "skipped"

            # 提交字幕上传任务
            if params["upload_subtitle"] and srt_content:
                srt_path = temp_dir / final_result["subtitle_file"]["filename"]
                upload_tasks.append(executor.submit(local_api_client.upload_file, str(srt_path)))
        
        # 处理上传结果
        for future in upload_tasks:
            try:
                # 假设 upload_file 成功时返回 Response，失败时返回 tuple
                response = future.result()
                
                is_srt_upload = False
                if hasattr(response, 'request') and response.request is not None and response.request.url is not None:
                     if ".srt" in unquote(response.request.url):
                        is_srt_upload = True

                if isinstance(response, requests.Response) and response.ok:
                    share_code = response.text.strip()
                    if is_srt_upload:
                        target_result = final_result["subtitle_file"]
                    else:
                        target_result = final_result["video_file"]
                    
                    target_result["status"] = "success"
                    target_result["share_code"] = share_code
                    target_result["share_link"] = f"http://{frp_server_addr}:{MIXFILE_REMOTE_PORT}/api/download/{quote(target_result['filename'])}?s={share_code}"
                else:
                    status_code, error_text = response if isinstance(response, tuple) else (response.status_code, response.text)
                    raise RuntimeError(f"上传失败。状态码: {status_code}, 错误: {error_text}")

            except Exception as e:
                log_system_event("error", f"任务 {task_id} 的文件上传失败: {e}", in_worker=True)
                if final_result["video_file"]["status"] == "pending":
                    final_result["video_file"]["status"] = "failed"
                    final_result["video_file"]["details"] = str(e)
                if final_result["subtitle_file"].get("status") == "success" and "share_code" not in final_result["subtitle_file"]:
                    final_result["subtitle_file"]["status"] = "upload_failed"
                    final_result["subtitle_file"]["details"] = str(e)

        # --- 6. 任务完成 ---
        result_data = {
            'type': 'task_result',
            'task_id': task_id,
            'status': 'success',
            'result': final_result,
            'details': '任务处理完成'
        }
        result_queue.put(result_data)
            
    except Exception as e:
        log_system_event("error", f"处理任务 {task_id} 时发生严重错误: {e}", in_worker=True)
        result_data = {
            'type': 'task_result',
            'task_id': task_id,
            'status': 'failed',
            'result': final_result, # 即使失败也返回部分结果
            'details': str(e)
        }
        result_queue.put(result_data)
    finally:
        # --- 7. 清理临时文件 ---
        try:
            shutil.rmtree(temp_dir)
            log_system_event("info", f"已清理任务 {task_id} 的临时目录。", in_worker=True)
        except Exception as e:
            log_system_event("error", f"清理任务 {task_id} 的临时目录失败: {e}", in_worker=True)

# =============================================================================
# --- 第 8 步: 多进程 Worker 与主程序 ---
# =============================================================================

def worker_process_loop(task_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue):
    """
    这是一个在独立子进程中运行的循环。
    它持续从任务队列中获取任务，调用处理器，并将结果放入结果队列。
    """
    log_system_event("info", "媒体处理工作进程已启动。", in_worker=True)
    while True:
        try:
            task_data = task_queue.get()
            if task_data is None: # 收到终止信号
                break
            log_system_event("info", f"工作进程接收到新任务: {task_data['task_id']}", in_worker=True)
            process_unified_task(task_data, result_queue)
        except KeyboardInterrupt:
            break
        except Exception as e:
            # 捕获循环中的意外错误，防止子进程崩溃
            log_system_event("critical", f"工作进程循环发生严重错误: {e}", in_worker=True)
    log_system_event("info", "媒体处理工作进程已关闭。", in_worker=True)

def result_processor_thread_loop(result_queue: multiprocessing.Queue):
    """
    这是一个在主进程中运行的后台线程。
    它持续从结果队列中获取来自子进程的数据，并更新主进程中的任务状态字典。
    """
    log_system_event("info", "结果处理线程已启动。")
    while True:
        try:
            # 使用带超时的get，防止永久阻塞，允许未来加入优雅退出逻辑
            result_data = result_queue.get(timeout=3600) 
            
            task_id = result_data['task_id']
            with tasks_lock:
                if task_id not in tasks:
                    continue # 如果任务已被清除，则忽略

                data_type = result_data['type']
                if data_type == 'status_update':
                    payload = result_data['payload']
                    tasks[task_id].update(payload)

                elif data_type == 'task_result':
                    tasks[task_id]['status'] = result_data['status']
                    tasks[task_id]['result'] = result_data['result']
                    tasks[task_id]['details'] = result_data['details']
                    tasks[task_id]['progress'] = 100
                    log_system_event("info", f"任务 {task_id} 已完成，状态: {result_data['status']}.")
        
        except QueueEmpty:
            continue # 超时是正常情况，继续循环
        except Exception as e:
            log_system_event("error", f"结果处理线程发生错误: {e}")

def main():
    #
    # 主执行函数，负责初始化和启动所有服务。
    #
    global api_client, subtitle_config_global, FRP_SERVER_ADDR, TASK_QUEUE, RESULT_QUEUE
    
    try:

        # --- 1. 启动前准备 ---
        log_system_event("info", "服务正在启动...")


        # 【核心修改】在 main() 函数中，替换整个 pip install 逻辑

        # 第一步：安装与Kaggle CUDA 12.1兼容的特定版本PyTorch (保持不变)
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

        # 第二步：安装所有其他库，包括新增的 speechbrain, tensorflow, demucs 等
        install_other_cmd = (
            "pip install -q pydantic pydub demucs "
            "speechbrain==0.5.16 "  # 固定 SpeechBrain 版本以保证兼容性
            "tensorflow tensorflow_hub "
            "faster-whisper@https://github.com/SYSTRAN/faster-whisper/archive/refs/heads/master.tar.gz "
            "google-generativeai requests psutil"
        )
        log_system_event("info", "正在安装其余依赖库...")
        subprocess.run(install_other_cmd, shell=True, check=True)
        log_system_event("info", "✅ 其余依赖库安装完成。")


        check_environment()
        
        # --- 设置多进程启动方法 ---
        # Kaggle环境推荐使用 'fork'
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
        
        worker = multiprocessing.Process(
            target=worker_process_loop,
            args=(TASK_QUEUE, RESULT_QUEUE),
            daemon=True
        )
        worker.start()
        
        # 启动结果处理线程
        result_thread = threading.Thread(
            target=result_processor_thread_loop,
            args=(RESULT_QUEUE,),
            daemon=True
        )
        result_thread.start()


        # --- 6. 启动 Flask API 服务 (必须在 if __name__ == '__main__': 块内启动) ---
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
            log_system_event("info", f"服务持续运行中... ({time.ctime()}) 工作进程状态: {'存活' if worker.is_alive() else '已退出'}")
            
    except (DecryptionError, RuntimeError, ValueError) as e:
        log_system_event("critical", f"程序启动过程中发生致命错误: {e}")
        log_system_event("critical", "服务无法启动，程序将终止。")
    except KeyboardInterrupt:
        log_system_event("info", "服务已手动停止。")
        if 'TASK_QUEUE' in globals() and TASK_QUEUE is not None:
            TASK_QUEUE.put(None) # 发送信号让子进程退出
    except Exception as e:
        log_system_event("critical", f"发生未知的致命错误: {e}")

if __name__ == '__main__':
    main()