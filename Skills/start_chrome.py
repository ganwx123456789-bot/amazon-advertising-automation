import subprocess, os, time, socket

CHROME_USER_DATA = r"C:\sel_chrome"
paths = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
]
chrome = next((p for p in paths if os.path.exists(p)), None)
if not chrome:
    print("Chrome not found")
else:
    # 找第一个没被占用的端口
    port = None
    for p in range(9222, 9231):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        if sock.connect_ex(("127.0.0.1", p)) != 0:
            port = p
            sock.close()
            break
        sock.close()
    if not port:
        print("ERROR: ports 9222-9230 all occupied")
    else:
        subprocess.Popen([chrome, f"--remote-debugging-port={port}", f"--user-data-dir={CHROME_USER_DATA}"])
        print(f"Chrome started on 127.0.0.1:{port}")
