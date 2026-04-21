"""
Chrome 调试端口自动探测模块
从 9222 到 9230 依次尝试，返回第一个可用的端口。
"""
import urllib.request
import json


def find_chrome_debug_port(start=9222, end=9230) -> int:
    """扫描端口范围，返回第一个有 CDP 响应的端口，找不到则抛异常"""
    for port in range(start, end + 1):
        try:
            url = f"http://127.0.0.1:{port}/json/version"
            req = urllib.request.urlopen(url, timeout=2)
            data = json.loads(req.read())
            if "webSocketDebuggerUrl" in data:
                print(f"[端口探测] ✅ 找到 Chrome 调试端口: {port}")
                return port
        except Exception:
            continue
    raise ConnectionError(
        f"[端口探测] ❌ 在 {start}-{end} 范围内未找到可用的 Chrome 调试端口。\n"
        "请先运行 start_chrome.py 启动 Chrome，或手动启动带 --remote-debugging-port 的 Chrome。"
    )


def get_cdp_url(start=9222, end=9230) -> str:
    """返回可用的 CDP 连接 URL，如 http://127.0.0.1:9224"""
    port = find_chrome_debug_port(start, end)
    return f"http://127.0.0.1:{port}"
