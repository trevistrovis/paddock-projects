import webbrowser
import time

SERVER_URL = 'http://192.168.3.108:8000/'

def open_browser():
    print(f"Connecting to Spec Search at: {SERVER_URL}")
    webbrowser.open(SERVER_URL)

if __name__ == '__main__':
    open_browser()