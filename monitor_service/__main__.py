import argparse,logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from .store import Store
from .server import Server

def main():
    parser=argparse.ArgumentParser(description='Локальный сервис обработки серверных показателей')
    parser.add_argument('--port',type=int,default=8765)
    parser.add_argument('--data-dir',type=Path,default=Path(__file__).resolve().parent/'data')
    parser.add_argument('--log-dir',type=Path,help='Папка доступных журналов; по умолчанию log_inputs внутри папки данных')
    args=parser.parse_args();args.data_dir.mkdir(parents=True,exist_ok=True)
    log=RotatingFileHandler(args.data_dir/'service.log',maxBytes=2_000_000,backupCount=3,encoding='utf-8')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[log])
    try:server=Server(('127.0.0.1',args.port),Store(args.data_dir/'monitor.sqlite3'),args.log_dir)
    except OSError as exc:
        print(f'Не удалось запустить сервис: {exc}. Возможно, он уже работает на этом адресе.');return
    print(f'Откройте http://127.0.0.1:{args.port} — Ctrl+C для остановки.',flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.stop_event.set();server.logs.close();server.bench.close();server.server_close()
if __name__=='__main__':main()
